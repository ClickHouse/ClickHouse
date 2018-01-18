#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

. $CURDIR/../shell_config.sh

expect $CURDIR/00532_pager_sigpipe.tcl && echo 'Ok' || echo 'Fail'
