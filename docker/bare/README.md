## The bare minimum ClickHouse Docker image.

It is intended as a showcase to check the amount of implicit dependencies of ClickHouse on the OS in addition to the OS kernel.

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

- Creation of `clickhouse` user to run the server.
- VOLUME for server.
- For most details, see other docker images for comparison.

The binary is statically linked with `musl`, so the `prepare` script copies no shared libraries, only the binary and `/etc/resolv.conf`, and even that file is optional: CA certificates are embedded into the binary and used when none are found on the filesystem, DNS works without `/etc/resolv.conf` (well-known public DNS resolvers are used when the file does not exist), and `localhost` resolves to loopback per RFC 6761 even without `/etc/hosts`.
