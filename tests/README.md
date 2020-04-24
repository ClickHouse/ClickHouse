## Tests

ClickHouse has several levels of tests

- **unit**
- **functional** - located in `queries` directory
- **integrational** - located in `integration` directory
- **stress** - runs lots of functional tests simultaneousluy
- **performance**

**clickhouse-test-server** folder contains files that are used to run clickhouse tests in temporary environment.
It can be run using CTest.
You can use `ninja check` or `make check` to run tests using ctest.

**config** folder contains separate parts of configuration files that are used in tests.
Tests make symlinks when set up their environment can use all of them or just several.
This directory is part of `clickhouse-test` deb package.

More information about testing is available in [docs](https://clickhouse.tech/docs/en/development/tests/)
