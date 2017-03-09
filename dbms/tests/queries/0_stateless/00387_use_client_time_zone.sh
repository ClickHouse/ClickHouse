#!/bin/sh -e

TZ=UTC clickhouse --client --use_client_time_zone=1 --query="SELECT toDateTime(1000000000)"
