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
* [docker compose](https://docs.docker.com/compose/) and additional python libraries. To install:

```bash
sudo -H pip install \
    PyMySQL \
    avro \
    cassandra-driver \
    confluent-kafka \
    dicttoxml \
    docker \
    grpcio \
    grpcio-tools \
    kafka-python \
    kazoo \
    minio \
    lz4 \
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
    pika \
    nats-py
```

(highly not recommended) If you really want to use OS packages on modern debian/ubuntu instead of "pip": `sudo apt install -y docker docker-compose-v2 python3-pytest python3-dicttoxml python3-docker python3-pymysql python3-protobuf python3-pymongo python3-tzlocal python3-kazoo python3-psycopg2 kafka-python python3-pytest-timeout python3-minio`

Some tests have other dependencies, e.g. spark. See docker/test/integration/runner/Dockerfile for how to install those. See docker/test/integration/runner/dockerd-entrypoint.sh for environment variables that need to be set (e.g. JAVA_PATH).

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


### Running with runner script

The only requirement is fresh configured docker and
docker pull clickhouse/integration-tests-runner

Notes:
* If you want to run integration tests without `sudo` you have to add your user to docker group `sudo usermod -aG docker $USER`. [More information](https://docs.docker.com/install/linux/linux-postinstall/) about docker configuration.
* If you already had run these tests without `./runner` script you may have problems with pytest cache. It can be removed with `rm -r __pycache__ .pytest_cache/`.
* Some tests maybe require a lot of resources (CPU, RAM, etc.). Better not try large tests like `test_distributed_ddl*` on your laptop.

You can run tests via `./runner` script and pass pytest arguments as last arg:
```bash
$ ./runner --binary $HOME/ClickHouse/programs/clickhouse  --odbc-bridge-binary $HOME/ClickHouse/programs/clickhouse-odbc-bridge --base-configs-dir $HOME/ClickHouse/programs/server/ 'test_ssl_cert_authentication -ss'
Start tests
====================================================================================================== test session starts ======================================================================================================
platform linux -- Python 3.8.10, pytest-7.1.2, pluggy-1.0.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /ClickHouse/tests/integration, configfile: pytest.ini
plugins: repeat-0.9.1, xdist-2.5.0, forked-1.4.0, order-1.0.0, timeout-2.1.0
timeout: 900.0s
timeout method: signal
timeout func_only: False
collected 4 items

test_ssl_cert_authentication/test.py::test_https Copy common default production configuration from /clickhouse-config. Files: config.xml, users.xml
PASSED
test_ssl_cert_authentication/test.py::test_https_wrong_cert PASSED
test_ssl_cert_authentication/test.py::test_https_non_ssl_auth PASSED
test_ssl_cert_authentication/test.py::test_create_user PASSED

================================================================================================= 4 passed in 118.58s (0:01:58) =================================================================================================

```

Path to binary and configs maybe specified via env variables:
```bash
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

### Parallel test execution

On the CI, we run a number of parallel runners (5 at the time of this writing), each on its own
Docker container. These runner containers spawn more containers for each test for the services
needed such as ZooKeeper, MySQL, PostgreSQL and minio, among others. This means that tests do not
share any services among them. Within each runner, tests are parallelized using
[pytest-xdist](https://pytest-xdist.readthedocs.io/en/stable/). We're using `--dist=loadfile` to
[distribute the load](https://pytest-xdist.readthedocs.io/en/stable/distribution.html). In the
documentation words: this guarantees that all tests in a file run in the same worker. This means
that any test within the same file will never execute their tests in parallel. They'll be executed
on the same worker one after the other.

If the test supports parallel and repeated execution, you can run a bunch of them in parallel to
look for flakiness. We use [pytest-repeat](https://pypi.org/project/pytest-repeat/) to set the
number of times we want to execute a test through the `--count` argument. Then, `-n` sets the number
of parallel workers for `pytest-xdist`.

```bash
$ export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$HOME/ClickHouse/programs/server/
$ export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$HOME/ClickHouse/programs/clickhouse
$ export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=$HOME/ClickHouse/programs/clickhouse-odbc-bridge
$ ./runner test_storage_s3_queue/test.py::test_max_set_age --count 10 -n 5
Start tests
=============================================================================== test session starts ================================================================================
platform linux -- Python 3.10.12, pytest-7.4.4, pluggy-1.5.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /ClickHouse/tests/integration
configfile: pytest.ini
plugins: reportlog-0.4.0, xdist-3.5.0, random-0.2, repeat-0.9.3, order-1.0.0, timeout-2.2.0
timeout: 900.0s
timeout method: signal
timeout func_only: False
5 workers [10 items]
scheduling tests via LoadScheduling

test_storage_s3_queue/test.py::test_max_set_age[9-10]
test_storage_s3_queue/test.py::test_max_set_age[7-10]
test_storage_s3_queue/test.py::test_max_set_age[5-10]
test_storage_s3_queue/test.py::test_max_set_age[1-10]
test_storage_s3_queue/test.py::test_max_set_age[3-10]
[gw3] [ 10%] PASSED test_storage_s3_queue/test.py::test_max_set_age[7-10]
test_storage_s3_queue/test.py::test_max_set_age[8-10]
[gw4] [ 20%] PASSED test_storage_s3_queue/test.py::test_max_set_age[9-10]
test_storage_s3_queue/test.py::test_max_set_age[10-10]
[gw0] [ 30%] PASSED test_storage_s3_queue/test.py::test_max_set_age[1-10]
test_storage_s3_queue/test.py::test_max_set_age[2-10]
[gw1] [ 40%] PASSED test_storage_s3_queue/test.py::test_max_set_age[3-10]
test_storage_s3_queue/test.py::test_max_set_age[4-10]
[gw2] [ 50%] PASSED test_storage_s3_queue/test.py::test_max_set_age[5-10]
test_storage_s3_queue/test.py::test_max_set_age[6-10]
[gw3] [ 60%] PASSED test_storage_s3_queue/test.py::test_max_set_age[8-10]
[gw4] [ 70%] PASSED test_storage_s3_queue/test.py::test_max_set_age[10-10]
[gw0] [ 80%] PASSED test_storage_s3_queue/test.py::test_max_set_age[2-10]
[gw1] [ 90%] PASSED test_storage_s3_queue/test.py::test_max_set_age[4-10]
[gw2] [100%] PASSED test_storage_s3_queue/test.py::test_max_set_age[6-10]
========================================================================== 10 passed in 120.65s (0:02:00) ==========================================================================
```

### Rebuilding the docker containers

The main container used for integration tests lives in `docker/test/integration/base/Dockerfile`. Rebuild it with
```
cd docker/test/integration/base
docker build -t clickhouse/integration-test .
```

The helper container used by the `runner` script is in `docker/test/integration/runner/Dockerfile`.
It can be rebuild with 

```
cd docker/test/integration/runner
docker build -t clickhouse/integration-test-runner .
```

If your docker configuration doesn't allow access to public internet with docker build command you may also need to add option --network=host if you rebuild image for a local integration testsing.

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

If tests failing for mysterious reasons, this may help:

```bash
sudo service docker stop
sudo bash -c 'rm -rf /var/lib/docker/*'
sudo service docker start
```

#### `iptables-nft`

On Ubuntu 20.10 and later in host network mode (default) one may encounter problem with nested containers not seeing each other. It happens because legacy and nftables rules are out of sync. Problem can be solved by:

```bash
sudo iptables -P FORWARD ACCEPT
```
