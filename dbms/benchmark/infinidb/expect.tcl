#!/bin/bash
#!/bin/expect

# Set timeout
set timeout 400

# Get arguments
set query [lindex $argv 0]

spawn /usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root

expect "mysql>"
send "use hits\r"

expect "mysql>"
send "$query\r"

expect "mysql>"
send "quit\r"

expect eof