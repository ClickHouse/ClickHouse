#!/usr/bin/env bash

TZ=Europe/Moscow clickhouse-local --query="SELECT toDateTime('1990-10-19 00:00:00')" 2>/dev/null
TZ=Asia/Colombo clickhouse-local --query="SELECT toDateTime('1990-10-19 00:00:00')" 2>/dev/null
TZ=Asia/Kathmandu clickhouse-local --query="SELECT toDateTime('1990-10-19 00:00:00')" 2>/dev/null
