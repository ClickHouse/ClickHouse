# ClickHouse configs for test environment

## How to use
If you want to run all tests from `tests/queries/0_stateless` and `test/queries/1_stateful` on your local machine you have to set up configs from this directory for your clickhouse-server. This most simple way is to install them using `install.sh` script.

## How to add new config

Just place file `.xml` with new config into appropriate directory and add `ln` command into `install.sh` script. After that CI will use this config in all tests runs.
