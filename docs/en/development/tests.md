# How to run ClickHouse tests

The `clickhouse-test` utility that is used for functional testing is written using Python 2.x.It also requires you to have some third-party packages:

```bash
$ pip install lxml termcolor
```

In a nutshell:

- Put the `clickhouse` program to `/usr/bin` (or `PATH`)
- Create a `clickhouse-client` symlink in `/usr/bin` pointing to `clickhouse`
- Start the `clickhouse` server
- `cd dbms/tests/`
- Run `./clickhouse-test`

## Example usage

Run `./clickhouse-test --help` to see available options.

To run tests without having to create a symlink or mess with `PATH`:

```bash
./clickhouse-test -c "../../build/dbms/src/Server/clickhouse --client"
```

To run a single test, i.e. `00395_nullable`:

```bash
./clickhouse-test 00395
```

