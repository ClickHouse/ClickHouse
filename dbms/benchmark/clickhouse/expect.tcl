#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn clickhouse-client --multiline;
expect ":) "
send "$query;\r";
expect ":) "
send "quit";
