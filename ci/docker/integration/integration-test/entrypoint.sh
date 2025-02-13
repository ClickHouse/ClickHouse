#!/usr/bin/env bash

ln -s "$(which clickhouse)" /usr/bin/chc
ln -s "$(which clickhouse)" /usr/bin/chl

exec "$@"
