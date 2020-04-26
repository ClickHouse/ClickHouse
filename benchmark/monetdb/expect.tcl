#!/usr/bin/env bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn mclient -u monetdb -d hits
expect "password:"
send "monetdb\r"

expect "sql>"
send "$query\r"

expect "sql>"
send "\\q\r"

expect eof