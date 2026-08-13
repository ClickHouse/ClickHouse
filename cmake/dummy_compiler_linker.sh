#!/bin/bash

next=0
for arg in "$@"; do
    if [[ $next -eq 0 ]]; then
        if [[ $arg == "-o" ]]; then
            next=1
            continue
        fi
    else
        echo "Generating dummy file $arg"
        touch $arg
        exit 0
    fi
done

