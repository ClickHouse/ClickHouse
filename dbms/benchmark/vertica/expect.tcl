#!/bin/bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn vsql -eU dbadmin

expect "dbadmin=>"
send "\\timing\r"

expect "dbadmin=>"
send "$query\r"

expect "dbadmin=>"
send "\\q\r"

expect eof