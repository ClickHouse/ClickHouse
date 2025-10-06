## ClickHouse integration tests

This directory contains tests that involve several ClickHouse instances, custom configs, ZooKeeper, etc.

### Running integration tests locally as CI job {#running-integration-tests-locally-as-ci-job}

You can reproduce CI integration test jobs locally using the same orchestration that CI uses.

- **Prerequisites**
  - Python 3
  - Docker
  - A ClickHouse server binary available to the job. Place the binary (or a symlink) in `./ci/tmp/`.

- **Run a CI job locally**
  ```bash
  python -m ci.praktika run "<JOB_NAME>"
  ```
  - **JOB_NAME** is the job name as it appears in the CI report, for example: `"Integration tests (amd_asan, 1/5)"`.

- **Run a specific test within a CI job**
  ```bash
  python -m ci.praktika run "Integration tests (amd_asan, 4/5)" \
    --test "test_storage_delta/test.py::test_single_log_file"

  # With specified test, the batch index in the job name (e.g., 4/5) is irrelevant locally, but
  # it must still match an actual job name from CI for the command to work.

  # The build token (e.g., amd_asan) is only a label used in CI reports. For
  # local runs, your architecture and build type are determined by your host
  # and the binary you provide.
  ```

No other dependencies are required beyond Python 3 and Docker.

### Running natively {#running-natively}

Prerequisites:
* Ubuntu 20.04 (Focal) or higher.
* [docker](https://www.docker.com/community-edition#/download). Minimum required API version: 1.25, check with `docker version`.

Install the latest Docker from:
https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository
Do not use Docker from your system repository.

* [pip](https://pypi.python.org/pypi/pip) and `libpq-dev`. To install: `sudo apt-get install python3-pip libpq-dev zlib1g-dev libcrypto++-dev libssl-dev libkrb5-dev python3-dev openjdk-17-jdk requests urllib3`
* [py.test](https://docs.pytest.org/) testing framework. To install: `sudo -H pip install pytest`
* [docker compose](https://docs.docker.com/compose/) and additional Python libraries. To install:

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

If you run tests with Spark, install Spark and add its `bin` directory to your `PATH`. See `ci/docker/integration/runner/Dockerfile` for details. Also set `JAVA_PATH` to the path of the Java binary.

If you want to run the tests under a non-privileged user, you must add this user to the `docker` group: `sudo usermod -aG docker $USER` and re-login.
(You must close all your sessions (for example, restart your computer))
To check that you have access to Docker, run `docker ps`.

Run the tests with the `pytest` command. Examples and useful flags:
```bash
pytest <tests_or_paths> \
  [-k <expr>] \
  [-n <numprocesses> --dist=loadfile] \
  [--count <count> --repeat-scope=function]
```

`-n <numprocesses>` — number of processes to run tests in parallel (CI uses 4 for parallelizable tests and 1 for the rest). Use `--dist=loadfile` when running in parallel. This guarantees that all tests in a file run on the same worker (tests within a file are not executed in parallel). See [pytest-xdist](https://pytest-xdist.readthedocs.io/en/stable/distribution.html) for details.
`--count <count>` — repeat a test multiple times; use with `--repeat-scope=function`. See [pytest-repeat](https://pypi.org/project/pytest-repeat/) for details.

Tests try to locate the server binary in the repository root and configs in `./programs/server/`. You can modify paths by setting these environment variables:
* `CLICKHOUSE_TESTS_SERVER_BIN_PATH`
* `CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH` (path to `clickhouse-odbc-bridge` binary)
* `CLICKHOUSE_TESTS_CLIENT_BIN_PATH`
* `CLICKHOUSE_TESTS_BASE_CONFIG_DIR` (path to `config.xml` and `users.xml`)

Please note that if you use a separate build (`ENABLE_CLICKHOUSE_ALL=OFF`), you need to build different components, including but not limited to `ENABLE_CLICKHOUSE_KEEPER=ON`. It's easier to use `ENABLE_CLICKHOUSE_ALL=ON`.


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

### Debug mode

Use this mode to attach a low‑level debugger to the ClickHouse server that runs inside the integration test containers.

1. Put a statically linked debugger binary at the root of your ClickHouse repository. We recommend [nnd](https://github.com/al13n321/nnd) (the only supported option currently).
   - Download: `curl -L -o nnd 'https://github.com/al13n321/nnd/releases/latest/download/nnd' && chmod +x nnd`
   - Note: nnd currently supports x86_64 only.

2. Add a `breakpoint()` to your test (e.g. in `test.py`) at the point you want to pause. This keeps the test (and server) alive so you can attach a debugger.

3. Run the test via the praktika CI wrapper (see [Running integration tests locally as CI job](#running-integration-tests-locally-as-ci-job)) and specify your test(s). For interactive debugging with pdb, make sure you run from a real TTY.

4. When the test hits the `breakpoint()`, you’ll see a Python pdb prompt similar to:
```bash
   >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PDB set_trace (IO-capturing turned off) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
> /ClickHouse/tests/integration/test_throttling/test.py(493)test_write_throttling()
-> assert_took(took, should_take)
(Pdb)
```

5. In a second terminal, source helper commands, then use the provided helpers:
```bash
   source /path/to/ClickHouse/tests/integration/runner-env.sh

USAGE:
   runner-client - Run clickhouse client inside an integration test
   runner-bash   - Open shell on a node inside an integration test
   runner-nnd    - Attach nnd debugger to a clickhouse server on a node inside an integration test
```

6. Use either a Container ID or a container name to connect to a node (the helper will list running containers and prompt you). Example:
   ```bash
   runner-client
CONTAINER ID   IMAGE                                      COMMAND                  CREATED          STATUS          PORTS                                                            NAMES
   867c67cc9957   clickhouse/integration-test:latest         "..."                    2s ago           Up 1s                                                                      rootteststoragedelta-node1-1
   ...
Enter ClickHouse Node CONTAINER ID or NAME (default: 867c67cc9957):
   ```

   After selecting a node, you can query it:
   ```sql
   node1 :) SELECT 1;
   ┌─1─┐
   │ 1 │
   └───┘
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
