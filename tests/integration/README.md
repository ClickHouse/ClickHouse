## ClickHouse integration tests

This directory contains tests that involve several ClickHouse instances, custom configs, ZooKeeper, etc.

### Running natively

Prerequisites:
* Ubuntu 20.04 (Focal) or higher.
* [docker](https://www.docker.com/community-edition#/download). Minimum required API version: 1.25, check with `docker version`.

You must install latest Docker from
https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository
Don't use Docker from your system repository.

* [pip](https://pypi.python.org/pypi/pip) and `libpq-dev`. To install: `sudo apt-get install python3-pip libpq-dev zlib1g-dev libcrypto++-dev libssl-dev libkrb5-dev python3-dev`
* [py.test](https://docs.pytest.org/) testing framework. To install: `sudo -H pip install pytest`
* [docker-compose](https://docs.docker.com/compose/) and additional python libraries. To install:

```
sudo -H pip install \
    PyMySQL \
    aerospike \
    avro \
    cassandra-driver \
    confluent-kafka \
    dicttoxml \
    docker \
    docker-compose \
    grpcio \
    grpcio-tools \
    kafka-python \
    kazoo \
    minio \
    protobuf \
    psycopg2-binary \
    pymongo \
    pytz \
    pytest \
    pytest-timeout \
    redis \
    tzlocal==2.1 \
    urllib3 \
    requests-kerberos \
    dict2xml \
    hypothesis \
    pyhdfs \
    pika
```

(highly not recommended) If you really want to use OS packages on modern debian/ubuntu instead of "pip": `sudo apt install -y docker docker-compose python3-pytest python3-dicttoxml python3-docker python3-pymysql python3-protobuf python3-pymongo python3-tzlocal python3-kazoo python3-psycopg2 kafka-python python3-pytest-timeout python3-minio`

If you want to run the tests under a non-privileged user, you must add this user to `docker` group: `sudo usermod -aG docker $USER` and re-login.
(You must close all your sessions (for example, restart your computer))
To check, that you have access to Docker, run `docker ps`.

Run the tests with the `pytest` command. To select which tests to run, use: `pytest -k <test_name_pattern>`

By default tests are run with system-wide client binary, server binary and base configs. To change that,
set the following environment variables:
* `CLICKHOUSE_TESTS_SERVER_BIN_PATH` to choose the server binary.
* `CLICKHOUSE_TESTS_CLIENT_BIN_PATH` to choose the client binary.
* `CLICKHOUSE_TESTS_BASE_CONFIG_DIR` to choose the directory from which base configs (`config.xml` and`users.xml`) are taken.

Please note that if you use separate build (`ENABLE_CLICKHOUSE_ALL=OFF`), you need to build different components, including but not limited to `ENABLE_CLICKHOUSE_LIBRARY_BRIDGE=ON ENABLE_CLICKHOUSE_ODBC_BRIDGE=ON ENABLE_CLICKHOUSE_KEEPER=ON`. So it is easier to use `ENABLE_CLICKHOUSE_ALL=ON`

For tests that use common docker compose files you may need to set up their path with environment variable: `DOCKER_COMPOSE_DIR=$HOME/ClickHouse/docker/test/integration/runner/compose`

### Running with runner script

The only requirement is fresh configured docker and
docker pull clickhouse/integration-tests-runner

Notes:
* If you want to run integration tests without `sudo` you have to add your user to docker group `sudo usermod -aG docker $USER`. [More information](https://docs.docker.com/install/linux/linux-postinstall/) about docker configuration.
* If you already had run these tests without `./runner` script you may have problems with pytest cache. It can be removed with `rm -r __pycache__ .pytest_cache/`.
* Some tests maybe require a lot of resources (CPU, RAM, etc.). Better not try large tests like `test_cluster_copier` or `test_distributed_ddl*` on your laptop.

You can run tests via `./runner` script and pass pytest arguments as last arg:
```
$ ./runner --binary $HOME/ClickHouse/programs/clickhouse  --bridge-binary $HOME/ClickHouse/programs/clickhouse-odbc-bridge --base-configs-dir $HOME/ClickHouse/programs/server/ 'test_odbc_interaction -ss'
Start tests
============================= test session starts ==============================
platform linux2 -- Python 2.7.15rc1, pytest-4.0.0, py-1.7.0, pluggy-0.8.0
rootdir: /ClickHouse/tests/integration, inifile: pytest.ini
collected 6 items

test_odbc_interaction/test.py Removing network clickhouse_default
...

Killing roottestodbcinteraction_node1_1     ... done
Killing roottestodbcinteraction_mysql1_1    ... done
Killing roottestodbcinteraction_postgres1_1 ... done
Removing roottestodbcinteraction_node1_1     ... done
Removing roottestodbcinteraction_mysql1_1    ... done
Removing roottestodbcinteraction_postgres1_1 ... done
Removing network roottestodbcinteraction_default

==================== 6 passed, 1 warnings in 95.21 seconds =====================

```

Path to binary and configs maybe specified via env variables:
```
$ export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$HOME/ClickHouse/programs/server/
$ export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$HOME/ClickHouse/programs/clickhouse
$ export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=$HOME/ClickHouse/programs/clickhouse-odbc-bridge
$ ./runner 'test_odbc_interaction'
$ # or ./runner '-v -ss'
Start tests
============================= test session starts ==============================
platform linux2 -- Python 2.7.15rc1, pytest-4.0.0, py-1.7.0, pluggy-0.8.0
rootdir: /ClickHouse/tests/integration, inifile: pytest.ini
collected 6 items

test_odbc_interaction/test.py ......                                     [100%]
==================== 6 passed, 1 warnings in 96.33 seconds =====================
```

You can just open shell inside a container by overwritting the command:
./runner --command=bash

### Rebuilding the docker containers

The main container used for integration tests lives in `docker/test/integration/base/Dockerfile`. Rebuild it with
```
cd docker/test/integration/base
docker build -t clickhouse/integration-test .
```

The helper container used by the `runner` script is in `docker/test/integration/runner/Dockerfile`.

### Adding new tests

To add new test named `foo`, create a directory `test_foo` with an empty `__init__.py` and a file
named `test.py` containing tests in it. All functions with names starting with `test` will become test cases.

`helpers` directory contains utilities for:
* Launching a ClickHouse cluster with or without ZooKeeper in docker containers.
* Sending queries to launched instances.
* Introducing network failures such as severing network link between two instances.

To assert that two TSV files must be equal, wrap them in the `TSV` class and use the regular `assert`
statement. Example: `assert TSV(result) == TSV(reference)`. In case the assertion fails, `pytest`
will automagically detect the types of variables and only the small diff of two files is printed.

### Troubleshooting

If tests failing for misterious reasons, this may help:

```
sudo service docker stop
sudo bash -c 'rm -rf /var/lib/docker/*'
sudo service docker start
```

#### `iptables-nft`

On Ubuntu 20.10 and later in host network mode (default) one may encounter problem with nested containers not seeing each other. It happens because legacy and nftables rules are out of sync. Problem can be solved by:

```
sudo iptables -P FORWARD ACCEPT
```
