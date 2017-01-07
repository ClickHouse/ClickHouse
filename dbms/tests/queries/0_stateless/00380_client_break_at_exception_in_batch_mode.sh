#!/usr/bin/env bash

clickhouse-client --multiquery --query="SELECT 1; SELECT xyz; SELECT 2;" 2> /dev/null || true;
