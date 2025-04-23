#!/usr/bin/env bash

../../../build/programs/clickhouse client --param-test "param-test OK" -q "SELECT {test:String}"
../../../build/programs/clickhouse client --param_test "param_test OK" -q "SELECT {test:String}"