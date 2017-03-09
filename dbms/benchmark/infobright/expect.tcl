#!/usr/bin/env bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn mysql-ib -u root -D hits

expect "mysql>"
send "$query\r"

expect "mysql>"
send "quit\r"

expect eof