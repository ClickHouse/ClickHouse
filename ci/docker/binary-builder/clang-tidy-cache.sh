#!/bin/bash

# Wrapper to have the output of clang-tidy-cache in a file.
# This is a workaround for CMake shadowing all output from clang-tidy except for errors.

output=$(clang-tidy-cache.py $@ 2>&1)
ret=$?
echo -e "clang-tidy-cache output for \"${!#}\":\n\t$output" >> /tmp/clang-tidy-cache.log
echo "$output"
exit $ret
