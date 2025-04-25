#!/bin/bash

# Echos the given architecture string normalized if it's supported, otherwise nothing
#   ARCH=$(fix_arch X86_64) # "x64"
function fix_arch {
    ARCH=$(echo $1 | tr '[:upper:]' '[:lower:]')
    case $ARCH in
      x86_64|x86|x64)    echo 'x64';;
      arm|arm64|aarch64) echo 'arm64';;
      universal)         echo 'universal';;
      *) >&2 "Unsupported architecture; \"$1\"";;
    esac
}

# Echos the local architecture if it's supported, otherwise nothing
#   ARCH=$(get_arch)
function get_arch {
    echo $(fix_arch $(uname -m) 2>/dev/null)
}