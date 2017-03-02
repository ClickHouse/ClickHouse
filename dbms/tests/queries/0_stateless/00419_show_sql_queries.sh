#!/bin/bash

clickhouse-client -q "SHOW PROCESSLIST" &>/dev/null
clickhouse-client -q "SHOW DATABASES" &>/dev/null
clickhouse-client -q "SHOW TABLES" &>/dev/null
