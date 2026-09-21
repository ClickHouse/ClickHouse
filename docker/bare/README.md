## The bare minimum ClickHouse Docker image.

It is intended as a showcase to check the amount of implicit dependencies of ClickHouse from the OS in addition to the OS kernel.

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
- most of the details, see other docker images for comparison;

A binary built with musl (`cmake -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-x86_64-musl.cmake`) needs none of the files installed by the `prepare` script: it is statically linked, CA certificates are embedded into the binary and used when none are found on the filesystem, DNS works without `/etc/resolv.conf` (well-known public DNS resolvers are used when the file does not exist), and `localhost` resolves to loopback per RFC 6761 even without `/etc/hosts`.
