#!/usr/bin/env sh

# Limits for a single translation unit of a sanitizer build:
# - RLIMIT_AS (virtual memory) to 15G,
# - RLIMIT_DATA (since RSS does not work since 2.6.x+) to 20G,
# - CPU time to 2000 seconds.
#
# The heaviest translation unit is `src/Interpreters/Aggregator.cpp`, which instantiates the
# aggregation loop for every key type: it peaks at ~11 GB of virtual memory (measured on aarch64,
# Clang 21, `-O3 -g`), most of it in the `Assignment Tracking Analysis` pass of the debug info
# generator. It used to peak at ~10 GB before the `unsigned-integer-overflow` check was enabled,
# which is to say it has been sitting right at the previous 10G limit for a while.

exec prlimit --as=15000000000 --data=20000000000 --cpu=2000 "$@"
