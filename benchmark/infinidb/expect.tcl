#!/usr/bin/env bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn mysql -u root

expect "mysql>"
send "use hits\r"

expect "mysql>"

send "$query\r"

expect "mysql>"

send "quit\r"

expect eof