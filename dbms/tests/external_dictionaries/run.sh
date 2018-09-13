#!/usr/bin/env bash


OS_NAME=`lsb_release -s -c`

if [ -z $(which python) ]; then
    sudo apt-get -y install python-lxml python-termcolor
fi

NO_MYSQL=0
NO_MONGO=0

for arg in "$@"; do
    if [ "$arg" = "--no_mysql" ]; then
        NO_MYSQL=1
    fi
    if [ "$arg" == "--no_mongo" ]; then
        NO_MONGO=1
    fi
done

# MySQL
if [ $NO_MYSQL -eq 1 ]; then
    echo "Not using MySQL"
else
    if [ -z $(which mysqld) ] || [ -z $(which mysqld) ]; then
        echo 'Installing MySQL'
        sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password '
        sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password '
        sudo apt-get -y --force-yes install mysql-server >/dev/null
        which mysqld >/dev/null
        if [ $? -ne 0 ]; then
            echo 'Failed installing mysql-server'
            exit -1
        fi

        echo 'Installed mysql-server'
    else
        echo 'MySQL already installed'
    fi

    MY_CNF=/etc/mysql/my.cnf
    LOCAL_INFILE_ENABLED=$(grep 'local-infile' $MY_CNF | cut -d= -f2)
    if [ -z $LOCAL_INFILE_ENABLED ] || [ $LOCAL_INFILE_ENABLED != 1 ]; then
        echo 'Enabling local-infile support'
        if [ -z "$(grep 'local-infile' $MY_CNF)" ]; then
            # add local-infile
            MY_CNF_PATTERN='/\[mysqld\]/alocal-infile = 1'
        else
            # edit local-infile just in case
            MY_CNF_PATTERN='s/local-infile.*/local-infile = 1/'
        fi
        sudo sed -i "$MY_CNF_PATTERN" $MY_CNF

        echo 'Enabled local-infile support for mysql'
        sudo service mysql stop
        sudo service mysql start
    else
        echo 'Support for local-infile already present'
        echo 'select 1;' | mysql $MYSQL_OPTIONS &>/dev/null
        if [ $? -ne 0 ]; then
            sudo service mysql start
        else
            echo 'MySQL already started'
        fi
    fi
fi

# MongoDB
if [ $NO_MONGO -eq 1 ]; then
    echo "Not using MongoDB"
else
    if [ -z $(which mongod) ] || [ -z $(which mongo) ]; then
        echo 'Installing MongoDB'

        if [ $OS_NAME == "trusty" ]; then
            MONGODB_ORG_VERSION=3.0.6
            sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10 &>/dev/null
            #echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list >/dev/null
            sudo apt-get update &>/dev/null
            sudo apt-get install -y mongodb-org=$MONGODB_ORG_VERSION >/dev/null

            which mongod >/dev/null
            if [ $? -ne 0 ]; then
                echo 'Failed installing mongodb-org'
                exit -1
            fi

            echo "Installed mongodb-org $MONGODB_ORG_VERSION"
        else
            sudo apt-get install -y mongodb
        fi

    fi

    echo | mongo &>/dev/null
    if [ $? -ne 0 ]; then
        sudo service mongod start
    else
        echo 'MongoDB already started'
    fi
fi

# ClickHouse
clickhouse-server &> clickhouse.log &
sleep 3
result=$(clickhouse-client --port 9001 --query 'select 1')
if [ $? -ne 0 ]; then
    echo 'Failed to start ClickHouse'
    exit -1
fi
echo 'Started ClickHouse server'
PID=$(grep PID clickhouse/status | sed 's/PID: //')
python ./generate_and_test.py "$@"
if [ $? -ne 0 ]; then
    echo 'Some test failed'
fi
kill -SIGTERM $PID
#wait $PID
echo 'Stopped ClickHouse server'
