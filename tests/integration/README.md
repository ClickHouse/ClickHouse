## ClickHouse integration tests

This directory contains tests that involve several ClickHouse instances, custom configs, ZooKeeper, etc. It is generally simpler to run tests with the [Runner](#running-with-runner-script) script.

### Running natively

Prerequisites:
* Ubuntu 20.04 (Focal) or higher.
* [docker](https://www.docker.com/community-edition#/download). Minimum required API version: 1.25, check with `docker version`.

You must install latest Docker from
https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository
Don't use Docker from your system repository.

* [pip](https://pypi.python.org/pypi/pip) and `libpq-dev`. To install: `sudo apt-get install python3-pip libpq-dev zlib1g-dev libcrypto++-dev libssl-dev libkrb5-dev python3-dev openjdk-17-jdk requests urllib3`
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
    pika \
    nats-py \
    pandas \
    numpy \
    jinja2 \
    pytest-xdist==2.4.0 \
    pyspark \
    azure-storage-blob \
    delta \
    paramiko \
    psycopg \
    pyarrow \
    boto3 \
    deltalake \
    snappy \
    pyiceberg \
    python-snappy \
    thrift
```

(highly not recommended) If you really want to use OS packages on modern debian/ubuntu instead of "pip": `sudo apt install -y docker.io docker-compose-v2 python3-pytest python3-dicttoxml python3-djocker python3-pymysql python3-protobuf python3-pymongo python3-tzlocal python3-kazoo python3-psycopg2 kafka-python3 python3-pytest-timeout python3-minio`

Some tests have other dependencies, e.g. spark. See docker/test/integration/runner/Dockerfile for how to install those. See docker/test/integration/runner/dockerd-entrypoint.sh for environment variables that need to be set (e.g. JAVA_PATH).

If you want to run the tests under a non-privileged user, you must add this user to `docker` group: `sudo usermod -aG docker $USER` and re-login.
(You must close all your sessions (for example, restart your computer))
To check, that you have access to Docker, run `docker ps`.

Run the tests with the `pytest` command. To select which tests to run, use: `pytest -k <test_name_pattern>`

By default tests tries to locate binaries and configs automatically, but if this does not work you may try to set:
* `CLICKHOUSE_TESTS_SERVER_BIN_PATH`
* `CLICKHOUSE_TESTS_CLIENT_BIN_PATH`
* `CLICKHOUSE_TESTS_BASE_CONFIG_DIR` (path to `config.xml` and`users.xml`)

Please note that if you use separate build (`ENABLE_CLICKHOUSE_ALL=OFF`), you need to build different components, including but not limited to `ENABLE_CLICKHOUSE_KEEPER=ON`. So it is easier to use `ENABLE_CLICKHOUSE_ALL=ON`


### Running with runner script

The only requirement is fresh configured docker and
docker pull clickhouse/integration-tests-runner

Notes:
* If you want to run integration tests without `sudo` you have to add your user to docker group `sudo usermod -aG docker $USER`. [More information](https://docs.docker.com/install/linux/linux-postinstall/) about docker configuration.
* If you already had run these tests without `./runner` script you may have problems with pytest cache. It can be removed with `rm -r __pycache__ .pytest_cache/`.
* Some tests maybe require a lot of resources (CPU, RAM, etc.). Better not try large tests like `test_distributed_ddl*` on your laptop.

You can run tests via `./runner` script and pass pytest arguments as last arg:
```bash
$ ./runner --binary $HOME/ClickHouse/programs/clickhouse --base-configs-dir $HOME/ClickHouse/programs/server/ -- test_ssl_cert_authentication -ss
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
docker build -t clickhouse/integration-tests-runner .
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

### Using pdb to break on assert

It is very handy to stop the test on assertion failure and play with it in
`python`, this can be done with `--pdb` switch, it will spawn `python`
interpreter in case of failures.

For native runs, simply `pytest --pdb <tests and other options>`

For docker runs, `runner <opts> -- <tests> --pdb`

### Debug mode

Here is how you run a debugger for the server under integration test:

1. Put the **_statically linked binary_** of your debugger into the ClickHouse root folder of your repo. The [nnd](https://github.com/al13n321/nnd) debugger is ideal for this purpose (and for ClickHouse debugging in general), it's currently the only supported option.
2. Go to the integration test `test.py` file and add a new line with a single `breakpoint()` command to debug specific point in your test (to make the server work forever and not die after the test).
3. Run the integration test with `runner` script as usual but add `--debug` option. It will produce helper shell command in stdout and start testing:
```bash
   # =====> DEBUG MODE <=====
    # ClickHouse root folder will be read-only mounted into /debug in all containers.
    # Tip: place a `breakpoint()` somewhere in your integration test python code before `runner`.
    # Open another shell and include:
        source /path/to/ClickHouse/tests/integration/runner-env.sh
    # =====> DEBUG MODE <=====
```
4. When the test hits a breakpoint, it will show the Python debugger prompt, which is useful by itself for debugging purposes. For example:
```bash
test_throttling/test.py::test_write_throttling[user_remote_throttling]
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PDB set_trace (IO-capturing turned off) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
> /ClickHouse/tests/integration/test_throttling/test.py(493)test_write_throttling()
-> assert_took(took, should_take)
(Pdb)
```
5. Open another shell and run the command there to set up the environment. Then run:
```bash
❯ source /path/to/ClickHouse/tests/integration/runner-env.sh

USAGE:
   runner-client - Run clickhouse client inside an integration test
   runner-bash   - Open shell on a node inside an integration test
   runner-nnd    - Attach nnd debugger to a clickhouse server on a node inside an integration test
```
6. You can use either Container ID or Container Name to log into the required node:
```
❯ runner-client
CONTAINER ID   IMAGE                                      COMMAND                  CREATED          STATUS          PORTS                                                            NAMES
867c67cc9957   clickhouse/integration-test:latest         "bash -c 'trap 'pkil…"   2 seconds ago    Up 1 second                                                                      rootteststoragedelta-node1-1
64b8744cb71a   clickhouse/integration-test:latest         "bash -c 'trap 'pkil…"   2 seconds ago    Up 1 second                                                                      rootteststoragedelta-node2-1
414187056a06   clickhouse/clickhouse-server:25.3.3.42     "bash -c 'trap 'pkil…"   2 seconds ago    Up 1 second     8123/tcp, 9000/tcp, 9009/tcp                                     rootteststoragedelta-node_old-1
f5c9aef75a04   clickhouse/integration-test:latest         "clickhouse server -…"   2 seconds ago    Up 1 second                                                                      rootteststoragedelta-node_with_environment_credentials-1
c7eae7f0c29d   mcr.microsoft.com/azure-storage/azurite    "docker-entrypoint.s…"   21 seconds ago   Up 20 seconds   10000-10002/tcp, 0.0.0.0:30000->30000/tcp, :::30000->30000/tcp   rootteststoragedelta-azurite1-1
8a81755f9358   minio/minio:RELEASE.2024-07-31T05-46-26Z   "/usr/bin/docker-ent…"   22 seconds ago   Up 21 seconds   9000-9001/tcp                                                    rootteststoragedelta-minio1-1
6f11a018f16c   clickhouse/python-bottle:latest            "python3"                22 seconds ago   Up 21 seconds   8080/tcp                                                         rootteststoragedelta-resolver-1
bf180f6b2f7d   clickhouse/s3-proxy                        "/docker-entrypoint.…"   22 seconds ago   Up 22 seconds   80/tcp, 443/tcp, 8080/tcp                                        rootteststoragedelta-proxy1-1
0c03e8d780e5   clickhouse/s3-proxy                        "/docker-entrypoint.…"   22 seconds ago   Up 22 seconds   80/tcp, 443/tcp, 8080/tcp                                        rootteststoragedelta-proxy2-1
1ef790b3251e   clickhouse/integration-test:latest         "clickhouse keeper -…"   28 seconds ago   Up 27 seconds                                                                    rootteststoragedelta-zoo2-1
a65b504a1e60   clickhouse/integration-test:latest         "clickhouse keeper -…"   28 seconds ago   Up 27 seconds                                                                    rootteststoragedelta-zoo3-1
51d20a4f40c4   clickhouse/integration-test:latest         "clickhouse keeper -…"   28 seconds ago   Up 28 seconds                                                                    rootteststoragedelta-zoo1-1
Enter ClickHouse Node CONTAINER ID or NAME (default: 867c67cc9957):
ClickHouse client version 25.7.1.1.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.7.1.
node1 :) select 1

SELECT 1

Query id: 7222639b-6d33-4237-85f4-ad4ee04e8691

   ┌─1─┐
1. │ 1 │
   └───┘

1 row in set. Elapsed: 0.001 sec.

node1 :) Bye.
```

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

### Slow internet connection problem

To download all dependencies, you can run `docker pull` manually. This will allow all dependencies to be downloaded without timeouts interrupting the tests.

```
export KERBERIZED_KAFKA_DIR=/tmp
export KERBERIZED_KAFKA_EXTERNAL_PORT=8080
export MYSQL_ROOT_HOST=%
export MYSQL_DOCKER_USER=root
export KERBEROS_KDC_DIR=/tmp
export AZURITE_PORT=10000
export KAFKA_EXTERNAL_PORT=8080
export SCHEMA_REGISTRY_EXTERNAL_PORT=8080
export SCHEMA_REGISTRY_AUTH_EXTERNAL_PORT=8080
export NGINX_EXTERNAL_PORT=8080
export COREDNS_CONFIG_DIR=/tmp/stub
export MYSQL_CLUSTER_DOCKER_USER=stub
export MYSQL_CLUSTER_ROOT_HOST=%
export MINIO_CERTS_DIR=/tmp/stub
export NGINX_EXTERNAL_PORT=8080
export MYSQL8_ROOT_HOST=%
export MYSQL8_DOCKER_USER=root
export ZOO_SECURE_CLIENT_PORT=2281
export RABBITMQ_COOKIE_FILE=/tmp/stub
export MONGO_SECURE_CONFIG_DIR=/tmp/stub
export PROMETHEUS_WRITER_PORT=8080
export PROMETHEUS_REMOTE_WRITE_HANDLER=/stub
export PROMETHEUS_REMOTE_READ_HANDLER=/stub
export PROMETHEUS_READER_PORT=8080
docker compose $(find ${HOME}/ClickHouse/tests/integration -name '*compose*yml' -exec echo --file {} ' ' \; ) pull
```


### IPv6 problem

If you have problems with network access to docker hub or other resources, then in this case you need to disable ipv6. To do this, open `sudo vim /etc/docker/daemon.json` and add:

```
{
  "ipv6": false
}
```
