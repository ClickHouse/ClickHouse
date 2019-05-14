## ClickHouse integration tests

This directory contains tests that involve several ClickHouse instances, custom configs, ZooKeeper, etc.

### Running natively

Prerequisites:
* Ubuntu 14.04 (Trusty) or higher.
* [docker](https://www.docker.com/community-edition#/download). Minimum required API version: 1.25, check with `docker version`.

You must install latest Docker from
https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository
Don't use Docker from your system repository.

* [pip](https://pypi.python.org/pypi/pip). To install: `sudo apt-get install python-pip`
* [py.test](https://docs.pytest.org/) testing framework. To install: `sudo -H pip install pytest`
* [docker-compose](https://docs.docker.com/compose/) and additional python libraries. To install: `sudo -H pip install docker-compose docker dicttoxml kazoo PyMySQL psycopg2 pymongo tzlocal kafka-python`

(highly not recommended) If you really want to use OS packages on modern debian/ubuntu instead of "pip": `sudo apt install -y docker docker-compose python-pytest python-dicttoxml python-docker python-pymysql python-pymongo python-tzlocal python-kazoo python-psycopg2 python-kafka`

If you want to run the tests under a non-privileged user, you must add this user to `docker` group: `sudo usermod -aG docker $USER` and re-login.
(You must close all your sessions (for example, restart your computer))
To check, that you have access to Docker, run `docker ps`.

Run the tests with the `pytest` command. To select which tests to run, use: `pytest -k <test_name_pattern>`

By default tests are run with system-wide client binary, server binary and base configs. To change that,
set the following environment variables:
* `CLICKHOUSE_TESTS_SERVER_BIN_PATH` to choose the server binary.
* `CLICKHOUSE_TESTS_CLIENT_BIN_PATH` to choose the client binary.
* `CLICKHOUSE_TESTS_BASE_CONFIG_DIR` to choose the directory from which base configs (`config.xml` and
  `users.xml`) are taken.


### Running with runner script

The only requirement is fresh docker configured docker.

Notes:
* If you want to run integration tests without `sudo` you have to add your user to docker group `sudo usermod -aG docker $USER`. [More information](https://docs.docker.com/install/linux/linux-postinstall/) about docker configuration.
* If you already had run these tests without `./runner` script you may have problems with pytest cache. It can be removed with `rm -r __pycache__ .pytest_cache/`.
* Some tests maybe require a lot of resources (CPU, RAM, etc.). Better not try large tests like `test_cluster_copier` or `test_distributed_ddl*` on your notebook.

You can run tests via `./runner` script and pass pytest arguments as last arg:
```
$ ./runner --binary $HOME/ClickHouse/dbms/programs/clickhouse --configs-dir $HOME/ClickHouse/dbms/programs/server/ 'test_odbc_interaction -ss'
Start tests
============================= test session starts ==============================
platform linux2 -- Python 2.7.15rc1, pytest-4.0.0, py-1.7.0, pluggy-0.8.0
rootdir: /ClickHouse/dbms/tests/integration, inifile: pytest.ini
collected 6 items

test_odbc_interaction/test.py Removing network clickhouse_default
...

Killing roottestodbcinteraction_node1_1     ... done
Killing roottestodbcinteraction_mysql1_1    ... done
Killing roottestodbcinteraction_postgres1_1 ... done
Removing roottestodbcinteraction_node1_1     ... done
Removing roottestodbcinteraction_mysql1_1    ... done
Removing roottestodbcinteraction_postgres1_1 ... done
Removing network roottestodbcinteraction_default

==================== 6 passed, 1 warnings in 95.21 seconds =====================

```

Path to binary and configs maybe specified via env variables:
```
$ export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$HOME/ClickHouse/dbms/programs/server/
$ export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$HOME/ClickHouse/dbms/programs/clickhouse
$ ./runner 'test_odbc_interaction'
Start tests
============================= test session starts ==============================
platform linux2 -- Python 2.7.15rc1, pytest-4.0.0, py-1.7.0, pluggy-0.8.0
rootdir: /ClickHouse/dbms/tests/integration, inifile: pytest.ini
collected 6 items

test_odbc_interaction/test.py ......                                     [100%]
==================== 6 passed, 1 warnings in 96.33 seconds =====================
```

### Adding new tests

To add new test named `foo`, create a directory `test_foo` with an empty `__init__.py` and a file
named `test.py` containing tests in it. All functions with names starting with `test` will become test cases.

`helpers` directory contains utilities for:
* Launching a ClickHouse cluster with or without ZooKeeper indocker containers.
* Sending queries to launched instances.
* Introducing network failures such as severing network link between two instances.

To assert that two TSV files must be equal, wrap them in the `TSV` class and use the regular `assert`
statement. Example: `assert TSV(result) == TSV(reference)`. In case the assertion fails, `pytest`
will automagically detect the types of variables and only the small diff of two files is printed.
