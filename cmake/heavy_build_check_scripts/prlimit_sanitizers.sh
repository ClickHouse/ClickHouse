#!/usr/bin/env sh

# set DATA (since RSS does not work since 2.6.x+) to 10G
# set VIRT (RLIMIT_AS) to 20G (DATA*2)
# set CPU time limit to 2000 seconds

exec prlimit --as=10000000000 --data=20000000000 --cpu=2000 "$@"
