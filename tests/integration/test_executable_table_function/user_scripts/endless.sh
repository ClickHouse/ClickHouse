#!/bin/bash

# Produces rows forever. A query that takes a few of them and stops leaves this command still
# writing; what ends it is the closing of its stdout, not a request it never reads.
yes "row"
