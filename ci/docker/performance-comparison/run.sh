#!/bin/bash

entry="/usr/share/clickhouse-test/performance/scripts/entrypoint.sh"
[ ! -e "$entry" ] && echo "ERROR: test scripts are not found" && exit 1

# Bind everything to one NUMA node, if there's more than one. Theoretically the
# node #0 should be less stable because of system interruptions. We bind
# randomly to node 1 or 0 to gather some statistics on that. We have to bind
# both servers and the tmpfs on which the database is stored. How to do it
# is unclear, but by default tmpfs uses
# 'process allocation policy', not sure which process but hopefully the one that
# writes to it, so just bind the downloader script as well.
# https://www.kernel.org/doc/Documentation/filesystems/tmpfs.txt
# Double-escaped backslashes are a tribute to the engineering wonder of docker --
# it gives '/bin/sh: 1: [bash,: not found' otherwise.
echo > compare.log
numactl --hardware | tee -a compare.log
node=$(( RANDOM % $(numactl --hardware | sed -n 's/^.*available:\(.*\)nodes.*$/\1/p') ));
echo Will bind to NUMA node $node | tee -a compare.log
numactl --cpunodebind=$node --membind=$node $entry
