# ClickHouse Server Docker Image

## What is ClickHouse?

We are the creators of the popular open-source column-oriented DBMS (columnar database management system) for online analytical processing (OLAP) which allows users to generate analytical reports using SQL queries in real-time.

ClickHouse works 100-1000x faster than traditional database management systems, and processes hundreds of millions to over a billion rows and tens of gigabytes of data per server per second.  With a widespread user base around the globe, the technology has received praise for its reliability, ease of use, and fault tolerance.

For more information and documentation see https://clickhouse.com/.

## How to use this image

### start server instance

```bash
docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

By default, ClickHouse will be accessible only via the Docker network. See the [networking section below](#networking).

By default, starting above server instance will be run as the `default` user without a password.

### connect to it from a native client

```bash
docker run -it --rm --link some-clickhouse-server:clickhouse-server --entrypoint clickhouse-client clickhouse/clickhouse-server --host clickhouse-server
# OR
docker exec -it some-clickhouse-server clickhouse-client
```

More information about the [ClickHouse client](https://clickhouse.com/docs/en/interfaces/cli/).

### connect to it using curl

```bash
echo "SELECT 'Hello, ClickHouse!'" | docker run -i --rm --link some-clickhouse-server:clickhouse-server curlimages/curl 'http://clickhouse-server:8123/?query=' -s --data-binary @-
```

More information about [ClickHouse HTTP Interface](https://clickhouse.com/docs/en/interfaces/http/).

### stopping / removing the container

```bash
docker stop some-clickhouse-server
docker rm some-clickhouse-server
```

### networking

You can expose your ClickHouse running in docker by [mapping a particular port](https://docs.docker.com/config/containers/container-networking/) from inside the container using host ports:

```bash
docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
echo 'SELECT version()' | curl 'http://localhost:18123/' --data-binary @-
```

```response
22.6.3.35
```

or by allowing the container to use [host ports directly](https://docs.docker.com/network/host/) using `--network=host` (also allows archiving better network performance):

```bash
docker run -d --network=host --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
echo 'SELECT version()' | curl 'http://localhost:8123/' --data-binary @-
```

```response
22.6.3.35
```

### Volumes

Typically you may want to mount the following folders inside your container to achieve persistency:

* `/var/lib/clickhouse/` - main folder where ClickHouse stores the data
* `/var/log/clickhouse-server/` - logs

```bash
docker run -d \
    -v $(realpath ./ch_data):/var/lib/clickhouse/ \
    -v $(realpath ./ch_logs):/var/log/clickhouse-server/ \
    --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

You may also want to mount:

* `/etc/clickhouse-server/config.d/*.xml` - files with server configuration adjustmenets
* `/etc/clickhouse-server/users.d/*.xml` - files with user settings adjustmenets
* `/docker-entrypoint-initdb.d/` - folder with database initialization scripts (see below).

### Linux capabilities

ClickHouse has some advanced functionality, which requires enabling several [Linux capabilities](https://man7.org/linux/man-pages/man7/capabilities.7.html).

These are optional and can be enabled using the following [docker command-line arguments](https://docs.docker.com/engine/reference/run/#runtime-privilege-and-linux-capabilities):

```bash
docker run -d \
    --cap-add=SYS_NICE --cap-add=NET_ADMIN --cap-add=IPC_LOCK \
    --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

## Configuration

The container exposes port 8123 for the [HTTP interface](https://clickhouse.com/docs/en/interfaces/http_interface/) and port 9000 for the [native client](https://clickhouse.com/docs/en/interfaces/tcp/).

ClickHouse configuration is represented with a file "config.xml" ([documentation](https://clickhouse.com/docs/en/operations/configuration_files/))

### Start server instance with custom configuration

```bash
docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 -v /path/to/your/config.xml:/etc/clickhouse-server/config.xml clickhouse/clickhouse-server
```

### Start server as a custom user

```bash
# $(pwd)/data/clickhouse should exist and be owned by current user
docker run --rm --user ${UID}:${GID} --name some-clickhouse-server --ulimit nofile=262144:262144 -v "$(pwd)/logs/clickhouse:/var/log/clickhouse-server" -v "$(pwd)/data/clickhouse:/var/lib/clickhouse" clickhouse/clickhouse-server
```

When you use the image with local directories mounted, you probably want to specify the user to maintain the proper file ownership. Use the `--user` argument and mount `/var/lib/clickhouse` and `/var/log/clickhouse-server` inside the container. Otherwise, the image will complain and not start.

### Start server from root (useful in case of userns enabled)

```bash
docker run --rm -e CLICKHOUSE_UID=0 -e CLICKHOUSE_GID=0 --name clickhouse-server-userns -v "$(pwd)/logs/clickhouse:/var/log/clickhouse-server" -v "$(pwd)/data/clickhouse:/var/lib/clickhouse" clickhouse/clickhouse-server
```

### How to create default database and user on starting

Sometimes you may want to create a user (user named `default` is used by default) and database on image start. You can do it using environment variables `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` and `CLICKHOUSE_PASSWORD`:

```bash
docker run --rm -e CLICKHOUSE_DB=my_database -e CLICKHOUSE_USER=username -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=password -p 9000:9000/tcp clickhouse/clickhouse-server
```

## How to extend this image

To perform additional initialization in an image derived from this one, add one or more `*.sql`, `*.sql.gz`, or `*.sh` scripts under `/docker-entrypoint-initdb.d`. After the entrypoint calls `initdb`, it will run any `*.sql` files, run any executable `*.sh` scripts, and source any non-executable `*.sh` scripts found in that directory to do further initialization before starting the service.
Also, you can provide environment variables `CLICKHOUSE_USER` & `CLICKHOUSE_PASSWORD` that will be used for clickhouse-client during initialization.

For example, to add an additional user and database, add the following to `/docker-entrypoint-initdb.d/init-db.sh`:

```bash
#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE docker;
    CREATE TABLE docker.docker (x Int32) ENGINE = Log;
EOSQL
```

## License

View [license information](https://github.com/ClickHouse/ClickHouse/blob/master/LICENSE) for the software contained in this image.

