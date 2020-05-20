#!/usr/bin/env bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn hive

expect "hive>"
send "$query;\r"

expect "hive>"
send "quit;\r"

expect eof