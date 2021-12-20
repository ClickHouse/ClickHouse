#!/usr/bin/env bash

# single query echo on
${CLICKHOUSE_CLIENT} --echo --query="DROP TABLE IF EXISTS echo_test_0" && \
# single query echo off
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS echo_test_1" && \
# multi query echo on
${CLICKHOUSE_CLIENT} --echo --multiquery --query="DROP TABLE IF EXISTS echo_test_2;DROP TABLE IF EXISTS echo_test_3" && \
# multi query echo off
${CLICKHOUSE_CLIENT} --multiquery --query="DROP TABLE IF EXISTS echo_test_4;DROP TABLE IF EXISTS echo_test_5" && \

true
