#!/bin/bash

# core.COMM.PID-TID
sysctl kernel.core_pattern='core.%e.%p-%P'
# ASAN doesn't work with suid_dumpable=2
sysctl fs.suid_dumpable=1

function run_with_retry()
{
    if [[ $- =~ e ]]; then
      set_e=true
    else
      set_e=false
    fi
    set +e

    local total_retries="$1"
    shift

    local retry=0

    until [ "$retry" -ge "$total_retries" ]
    do
        if "$@"; then
            if $set_e; then
              set -e
            fi
            return
        else
            retry=$((retry + 1))
            sleep 5
        fi
    done

    echo "Command '$*' failed after $total_retries retries, exiting"
    exit 1
}

function fn_exists() {
    declare -F "$1" > /dev/null;
}

function collect_core_dumps()
{
  find . -type f -maxdepth 1 -name 'core.*' | while read -r core; do
      zstd --threads=0 "$core"
      mv "$core.zst" /test_output/
  done
}

# vi: ft=bash
