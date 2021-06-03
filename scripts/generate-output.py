#!/usr/bin/env python

import argparse
import pandas as pd
import sys

class Output:

    # open log file
    def __init__(self, input, output):
        
        self.output = open(output, 'w')

        df = pd.read_csv(input, header=None, delimiter=r'\s+')
        df.columns = ['remote_host', 'rfc931', 'authuser', 'date', 'date2', 'request', 'status_code', 'bytes']
        df['date'] = df['date'] + " " + df['date2']
        del df['date2']
        new = (df['request'].str.split("\s", expand=True))
        df['method'], df['resource'], df['protocol'] = new[0], new[1], new[2]
        self.df = df
        sys.stdout = self.output
        #print(self.df.iloc[0])

    # Total number of requests
    def get_num_requests(self):
        return self.df.shape[0]
        
    # Total data transmitted over all requests
    def get_total_data(self):
        return self.df['bytes'].sum()
    
    def print_total_data(self, num_bytes):
        #print(num_bytes)
        #B
        if num_bytes < 1024:
            return "Total Data transmitted: {}{}".format(num_bytes, "Bytes")
        #KB
        if num_bytes < 1024 ** 2:
            return "Total Data transmitted: {:.1f}{}".format((num_bytes / 1024), "KB")

        #mB
        if num_bytes < 1024 ** 3:
            return "Total Data transmitted: {:.1f}{}".format((num_bytes / 1024 ** 2), "MB")

        #GB
        if num_bytes < 1024 ** 4:
            return "Total Data transmitted: {:.1f}{}".format((num_bytes / 1024 ** 3), "GB")

        #TB
        if num_bytes < 1024 ** 5:
            return "Total Data transmitted: {:.1f}{}".format((num_bytes / 1024 ** 4), "TB")


    # Most requested resource
    #not sure if only GET is wanted, or both POST and GET
    def get_most_requested(self):
        return self.df['resource'].value_counts().idxmax()

    # total number of requests for this resource
    def get_num_most_requested(self):
        return self.df['resource'].value_counts().max()

    # percentage of requests for this resource
    def get_most_requested_percent(self, num):
        return 100 * num / self.df['resource'].value_counts().sum()

    def print_most_requested(self):
        most_requested = self.get_most_requested()
        print("Most requested resource: {}".format(most_requested))
        num_most_requested = self.get_num_most_requested()
        print("Total requests for {}: {}".format(most_requested, num_most_requested))
        most_requested_percent = self.get_most_requested_percent(num_most_requested)
        print("Percentage of requests for {}: {:.10f}".format(most_requested, most_requested_percent))

    # Remote host with the most requests
    def get_remote_host_most_requested(self):
        return self.df['remote_host'].value_counts().idxmax()

    # total number of requests from this remote host
    def get_remote_host_num_most_requested(self):
        return self.df['remote_host'].value_counts().max()

    # percentage of requests for this resource
    def get_remote_host_most_requested_percent(self, num):
        return 100 * num / self.df['resource'].value_counts().sum()

    def print_remote_host_most_requested(self):
        remote_host_most_requested = self.get_remote_host_most_requested()
        print("Remote host with the most requests: {}".format(remote_host_most_requested))
        remote_host_num_most_requested = self.get_remote_host_num_most_requested()
        print("Total requests from {}: {}".format(remote_host_most_requested, remote_host_num_most_requested))
        remote_host_most_requested_percent = self.get_remote_host_most_requested_percent(remote_host_num_most_requested)
        print("Percentage of requests from {}: {:.10f}".format(remote_host_most_requested, remote_host_most_requested_percent))

    # Percentages of each class of HTTP status code (1xx, 2xx, 3xx, 4xx, 5xx)
    def print_status_code_percentages(self):
        bins = [100,199.9,299.9,399.9,499.9,599.9]
        bins = self.df['status_code'].value_counts(bins=bins, sort=False)
        #print(bins)
        #print(100*bins[200]/bins.sum())
        for i in range(1,6):
            print("Percentage of {}xx requests: {:.10f}".format(i, 100*bins[i*100]/bins.sum()))


def init():
    # Parse the arguments
    parser = argparse.ArgumentParser(
        description="retrieves logfile",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('-i', '--input',  help='log file to parse', required=True)
    parser.add_argument('-o', '--output', help='log file to print to', required=True)
    args = parser.parse_args()

   
    return args

args = init()
OUTPUT = Output(args.input, args.output)
print("Total Requests: {}".format(OUTPUT.get_num_requests()))
print(OUTPUT.print_total_data(OUTPUT.get_total_data()))
OUTPUT.print_most_requested()
OUTPUT.print_remote_host_most_requested()
OUTPUT.print_status_code_percentages()
OUTPUT.output.close()