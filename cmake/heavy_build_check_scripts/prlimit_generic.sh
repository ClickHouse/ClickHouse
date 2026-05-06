#!/usr/bin/env sh

# set DATA (since RSS does not work since 2.6.x+) to 5G
# set VIRT (RLIMIT_AS) to 10G (DATA*2)
# set CPU time limit to 1000 seconds

exec prlimit --as=5000000000 --data=10000000000 --cpu=1000 "$@"
