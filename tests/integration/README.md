# ClickHouse Integration Tests

This directory contains integration tests for ClickHouse, involving multiple instances, custom configurations, ZooKeeper, and more. These tests help ensure robust distributed behavior and compatibility with external systems.

## Running Integration Tests Locally as CI Job

You can reproduce CI integration test jobs locally using the same orchestration as CI.

### Prerequisites
- Python 3 (standard library only)
- Docker
- ClickHouse server binary available to the job. The runner searches in this order and uses the first found:
  - `./ci/tmp/clickhouse`
  - `./build/programs/clickhouse`
  - `./clickhouse`

### Run a CI Job Locally
```bash
python -m ci.praktika run "<JOB_NAME>"
```
- Always quote the job name exactly as in the CI report (it may contain spaces and commas), e.g.: `"Integration tests (amd_asan, 1/5)"`.

### Run a Specific Test Within a CI Job
```bash
python -m ci.praktika run "Integration tests (amd_binary, 4/5)" \
  --test "test_named_collections/"
```
- With `--test`, the batch index in the job name (e.g., `4/5`) is irrelevant locally, but the job name must match an actual CI job to select the right configuration.
- You can pass multiple test selectors:
  ```bash
  python -m ci.praktika run "Integration tests (amd_binary, 4/5)" \
    --test "test_named_collections/test.py::test_default_access" "test_multiple_disks/"
  ```
- Tip: For local runs with `--test`, the batch index and build flavor are not required. You can use the alias `integration`:
  ```bash
  python -m ci.praktika run "integration" --test <selectors>
  ```
  Praktika will pick a suitable job configuration for individual tests. Note: jobs with old analyzer or distributed plan still require the full, exact CI job name.

### Additional Customization Options
- `--count N` to repeat each test N times (`--count` is passed to pytest with `--repeat-scope=function`).
- `--debug` to open the Python debug console on exception (`--pdb` is passed to pytest).
- `--path PATH` custom ClickHouse server binary location (if not in default locations).
- `--path_1 PATH` custom path to the ClickHouse server config directory (if not in `./programs/server/config/`).
- `--workers N` to override automatic calculation of the recommended maximum number of parallel pytest workers. The value is passed to pytest-xdist as `-n N`. Use a lower number on resource-constrained machines or increase it to utilize more CPU cores.
## Running Natively

### Prerequisites
- Ubuntu 20.04 (Focal) or higher
- [Docker](https://www.docker.com/community-edition#/download) (API version ≥ 1.25; check with `docker version`).
- Latest Docker from [official docs](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository). Do not use system repository Docker.
- [pip](https://pypi.python.org/pypi/pip) and `libpq-dev`. Install with:
  ```bash
  sudo apt-get install python3-pip libpq-dev zlib1g-dev libcrypto++-dev libssl-dev libkrb5-dev python3-dev openjdk-17-jdk requests urllib3
  ```
- [pytest](https://docs.pytest.org/) testing framework:
  ```bash
  sudo -H pip install pytest
  ```
- [docker compose](https://docs.docker.com/compose/) and additional Python libraries:
  ```bash
  sudo -H pip install \
      PyMySQL avro cassandra-driver confluent-kafka dicttoxml docker grpcio grpcio-tools kafka-python kazoo minio lz4 protobuf psycopg2-binary pymongo pytz pytest pytest-timeout redis tzlocal==2.1 urllib3 requests-kerberos dict2xml hypothesis pika nats-py pandas numpy jinja2 pytest-xdist==2.4.0 pyspark azure-storage-blob delta paramiko psycopg pyarrow boto3 deltalake snappy pyiceberg python-snappy thrift
  ```
- For Spark tests, install Spark and add its `bin` directory to your `PATH`. See `ci/docker/integration/runner/Dockerfile` for details. Set `JAVA_PATH` to the Java binary path.
- To run tests as a non-privileged user, add the user to the `docker` group:
  ```bash
  sudo usermod -aG docker $USER
  # Re-login or restart your computer
  ```
  Check Docker access with `docker ps`.

### Running Tests
Run tests with `pytest`. Examples and useful flags:
```bash
pytest <tests_or_paths> \
  [-k <expr>] \
  [-n <numprocesses> --dist=loadfile] \
  [--count <count> --repeat-scope=function]
```
- `-n <numprocesses>`: Number of parallel processes (CI uses 4 for parallelizable tests, 1 for others). Use `--dist=loadfile` for parallel runs to keep tests in a file on the same worker. See [pytest-xdist](https://pytest-xdist.readthedocs.io/en/stable/distribution.html).
- `--count <count>`: Repeat a test multiple times; use with `--repeat-scope=function`. See [pytest-repeat](https://pypi.org/project/pytest-repeat/).

Tests locate the server binary in the repository root and configs in `./programs/server/`. You can override paths with these environment variables:
- `CLICKHOUSE_TESTS_SERVER_BIN_PATH`
- `CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH` (path to `clickhouse-odbc-bridge` binary)
- `CLICKHOUSE_TESTS_CLIENT_BIN_PATH`
- `CLICKHOUSE_TESTS_BASE_CONFIG_DIR` (path to `config.xml` and `users.xml`)

If using a separate build (`ENABLE_CLICKHOUSE_ALL=OFF`), build all required components (e.g., `ENABLE_CLICKHOUSE_KEEPER=ON`). Using `ENABLE_CLICKHOUSE_ALL=ON` is easier.

## Adding New Tests

To add a new test named `foo`, create a directory `test_foo` with an empty `__init__.py` and a `test.py` file containing your tests. All functions starting with `test` are test cases.

The `helpers` directory provides utilities for:
- Launching a ClickHouse cluster (with/without ZooKeeper) in Docker containers.
- Sending queries to instances.
- Simulating network failures (e.g., severing links between instances).

To compare TSV files, wrap them in the `TSV` class and use `assert`, e.g.:
```python
assert TSV(result) == TSV(reference)
```
On failure, pytest shows a concise diff.

## Using pdb to Break on Assert

To stop on assertion failure and debug interactively, use the `--pdb` switch:
```bash
pytest --pdb <tests and options>
```

## Debug Mode

Use debug mode to attach a low-level debugger to the ClickHouse server inside integration test containers.

1. Place a statically linked debugger binary (recommended: [nnd](https://github.com/al13n321/nnd), x86_64 only) at the repository root:
   ```bash
   curl -L -o nnd 'https://github.com/al13n321/nnd/releases/latest/download/nnd' && chmod +x nnd
   ```
2. Add a `breakpoint()` in your test (e.g., in `test.py`) where you want to pause.
3. Run the test via the praktika CI wrapper and specify your test(s). For interactive debugging, use a real TTY.
4. When the test hits `breakpoint()`, you'll see a Python pdb prompt:
5. In a second terminal, source helper commands and use the provided helpers:
   ```bash
   source /path/to/ClickHouse/tests/integration/runner-env.sh
   # USAGE:
   # runner-client - Run clickhouse client inside an integration test
   # runner-bash   - Open shell on a node inside an integration test
   # runner-nnd    - Attach nnd debugger to a clickhouse server on a node inside an integration test
   ```
6. Use a container ID or name to connect to a node (the helper lists running containers and prompts you):
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

## Troubleshooting

If tests fail for mysterious reasons:
```bash
sudo service docker stop
sudo bash -c 'rm -rf /var/lib/docker/*'
sudo service docker start
```

### iptables-nft Issue
On Ubuntu 20.10+ in host network mode, nested containers may not see each other due to legacy/nftables rule sync issues. Fix with:
```bash
sudo iptables -P FORWARD ACCEPT
```

### Slow Internet Connection

To download all dependencies in advance:
```bash
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

### IPv6 Problem

If you have network access issues to Docker Hub or other resources, disable IPv6:
```bash
sudo vim /etc/docker/daemon.json
# Add:
{
  "ipv6": false
}
```
