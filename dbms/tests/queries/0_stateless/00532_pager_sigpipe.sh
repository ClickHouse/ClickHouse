#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

expect $CURDIR/00532_pager_sigpipe.tcl && echo 'Ok' || echo 'Fail'
