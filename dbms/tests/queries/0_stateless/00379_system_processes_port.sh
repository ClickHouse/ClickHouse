#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

curl -sS --local-port 1390 'http://localhost:8123?query=SELECT%20port%20FROM%20system.processes%20ORDER%20BY%20elapsed%20LIMIT%201'
