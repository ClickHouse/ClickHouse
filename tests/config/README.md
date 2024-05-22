# ClickHouse configs for test environment

## How to use
CI use these configs in all checks installing them with `install.sh` script. If you want to run all tests from `tests/queries/0_stateless` and `test/queries/1_stateful` on your local machine you have to set up configs from this directory for your `clickhouse-server`. The easiest way is to install them using `install.sh` script. Another option is to copy files into your clickhouse config directory.

## How to add new config

Place file `.xml` with new config in the appropriate directory and add `ln` command into `install.sh` script. CI will use then this config in all test runs.
