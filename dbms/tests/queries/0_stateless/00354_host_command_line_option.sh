#!/bin/sh

clickhouse-client --host=localhost --query="SELECT 1";
clickhouse-client --host localhost --query "SELECT 1";
clickhouse-client -hlocalhost -q"SELECT 1";
