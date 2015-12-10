#/bin/sh

# MySQL
if [ -z $(which mysqld) ]; then
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
    sudo /etc/init.d/mysql stop
    sudo /etc/init.d/mysql start
else
    echo 'Support for local-infile already present'
    echo 'select 1;' | mysql &>/dev/null
    if [ $? -ne 0 ]; then
        sudo /etc/init.d/mysql start
    else
        echo 'MySQL already started'
    fi
fi

# MongoDB
if [ -z $(which mongod) ]; then
    echo 'Installing MongoDB'
    MONGODB_ORG_VERSION=3.0.6
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10 &>/dev/null
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list >/dev/null
    sudo apt-get update &>/dev/null
    sudo apt-get install -y mongodb-org=$MONGODB_ORG_VERSION >/dev/null
    which mongod >/dev/null
    if [ $? -ne 0 ]; then
        echo 'Failed installing mongodb-org'
        exit -1
    fi

    echo "Installed mongodb-org $MONGODB_ORG_VERSION"
fi

echo | mongo &>/dev/null
if [ $? -ne 0 ]; then
    sudo service mongod start
else
    echo 'MongoDB already started'
fi

# ClickHouse
clickhouse-server &>/dev/null &
sleep 3
result=$(clickhouse-client --port 9001 --query 'select 1')
if [ $? -ne 0 ]; then
    echo 'Failed to start ClickHouse'
    exit -1
fi
echo 'Started ClickHouse server'
PID=$(grep PID clickhouse/status | sed 's/PID: //')
python ./generate_and_test.py
if [ $? -ne 0 ]; then
    echo 'Some test failed'
fi
kill -SIGTERM $PID
#wait $PID
echo 'Stopped ClickHouse server'
