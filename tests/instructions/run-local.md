

## Requires Linux

Docker only supports IPV6 on Linux

## Enable IPV6 in Docker

Add or edit `/etc/docker/daemon.json`.  If you are creating a new file, then the file
should contain this content.  If you are editing an existing file, then add this content
to the existing content.  For more information, see the [Docker docs](https://docs.docker.com/config/daemon/ipv6/).

```json
{
  "ipv6": true,
  "fixed-cidr-v6": "2001:db8:1::/64"
}
```

## Volumes to be mounted in the container

There are four volumes mounted from your machine into the container:
- The directory containing the ClickHouse `deb` files
- The `tests` dir from the ClickHouse GitHub repo
- The directory where test logs will be written to
- The directory where server logs will be written to

You should create the four directories and replace the left-hand side of the volume 
statements with the path to your directories.

## Script to launch the container

TODO: Split run.sh into `install.sh` and `test.sh` so that the install can be done once, and 
then individual tests can be run instead of all.

```bash
docker run -ti \
  --volume=/home/droscigno/Downloads/clickhouse-debs:/package_folder \
  --volume=/home/droscigno/GitHub/Docs/ClickHouse/tests:/usr/share/clickhouse-test \
  --volume=/home/droscigno/tmp/test_output:/test_output \
  --volume=/home/droscigno/tmp/server_log:/var/log/clickhouse-server \
  --cap-add=SYS_PTRACE \
  -e MAX_RUN_TIME=9720 \
  -e S3_URL="https://s3.amazonaws.com/clickhouse-datasets" \
  -e RUN_BY_HASH_NUM=2 \
  -e RUN_BY_HASH_TOTAL=4 \
  -e USE_DATABASE_REPLICATED=1 \
  -e ADDITIONAL_OPTIONS="--hung-check --print-time" \
  clickhouse/stateless-test:latest \
  bash
```

## Deploy ClickHouse and run the tests

You should be at a `root` prompt in the container.  The container now has the `debs`, the 
test configs and scripts, and logging directories mounted as volumes.  Run `/run.sh` and
watch as this software is installed and configured:

- Minio
- ClickHouse
- ClickHouse Keeper
- etc.

The install and configuration takes time, let it go.  Once the install is finished the tests
will begin.  You can follow along with the status by looking in the log dirs, which are
mounted from your system.  In the configuration file above these are in `~/tmp/test_output/`
and `~/tmp/server_log/`:

```
  --volume=/home/droscigno/tmp/test_output:/test_output \
  --volume=/home/droscigno/tmp/server_log:/var/log/clickhouse-server \
```

## Explore the test results
Once the testing ends the query_log, log files, etc. will be compressed and deposited in
the `test_output/` directory, and can be examined there.  Try using `clickhouse-local` to
examine the query_log.

```bash
clickhouse-local --query \
"describe file('./query_log.tsv', 'TSVWithNames')"
```

```bash
clickhouse-local --query \
"select query from file('./query_log.tsv', 'TSVWithNames') order by event_time"
```
