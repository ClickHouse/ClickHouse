## The bare minimum ClickHouse Docker image.

It is intented as a showcase to check the amount of implicit dependencies of ClickHouse from the OS in addition to the OS kernel.

Example usage:

```
./prepare
docker build --tag clickhouse-bare .
```

Run clickhouse-local:
```
docker run -it --rm --network host clickhouse-bare /clickhouse local --query "SELECT 1"
```

Run clickhouse-client in interactive mode:
```
docker run -it --rm --network host clickhouse-bare /clickhouse client
```

Run clickhouse-server:
```
docker run -it --rm --network host clickhouse-bare /clickhouse server
```

It can be also run in chroot instead of Docker (first edit the `prepare` script to enable `proc`):

```
sudo chroot . /clickhouse server
```

## What does it miss?

- creation of `clickhouse` user to run the server;
- VOLUME for server;
- most of the details, see other docker images for comparison.
