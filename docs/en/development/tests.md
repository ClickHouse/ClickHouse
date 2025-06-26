---
description: 'Guide to testing ClickHouse and running the test suite'
sidebar_label: 'Testing'
sidebar_position: 40
slug: /development/tests
title: 'Testing ClickHouse'
---

# Testing ClickHouse

## Functional Tests {#functional-tests}

Functional tests are the most simple and convenient to use.
Most of ClickHouse features can be tested with functional tests and they are mandatory to use for every change in ClickHouse code that can be tested that way.

Each functional test sends one or multiple queries to the running ClickHouse server and compares the result with reference.

Tests are located in `queries` directory.
There are two subdirectories: `stateless` and `stateful`.
- Stateless tests run queries without any preloaded test data - they often create small synthetic datasets on the fly, within the test itself.
- Stateful tests require preloaded test data from ClickHouse and it is available to general public. See [stateful test in continuous integration](continuous-integration.md#functional-stateful-tests).

Each test can be one of two types: `.sql` and `.sh`.
- An `.sql` test is the simple SQL script that is piped to `clickhouse-client`.
- An `.sh` test is a script that is run by itself.

SQL tests are generally preferable to `.sh` tests.
You should use `.sh` tests only when you have to test some feature that cannot be exercised from pure SQL, such as piping some input data into `clickhouse-client` or testing `clickhouse-local`.

:::note
A common mistake when testing data types `DateTime` and `DateTime64` is assuming that the server uses a specific time zone (e.g. "UTC"). This is not the case, time zones in CI test runs
are deliberately randomized. The easiest workaround is to specify the time zone for test values explicitly, e.g. `toDateTime64(val, 3, 'Europe/Amsterdam')`.
:::

### Running a Test Locally {#running-a-test-locally}

Start the ClickHouse server locally, listening on the default port (9000).
To run, for example, the test `01428_hash_set_nan_key`, change to the repository folder and run the following command:

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

Test results (`stderr` and `stdout`) are written to files `01428_hash_set_nan_key.[stderr|stdout]` which are located next the test itself (for `queries/0_stateless/foo.sql`, the output will be in `queries/0_stateless/foo.stdout`).

See `tests/clickhouse-test --help` for all options of `clickhouse-test`.
You can run all tests or run subset of tests by providing a filter for test names: `./clickhouse-test substring`.
There are also options to run tests in parallel or in random order.

### Adding a New Test {#adding-a-new-test}

To add new test, first create a `.sql` or `.sh` file in `queries/0_stateless` directory.
Then generate the corresponding `.reference` file using `clickhouse-client < 12345_test.sql > 12345_test.reference` or `./12345_test.sh > ./12345_test.reference`.

Tests should only create, drop, select from, etc. tables in database `test` which is automatically created beforehand.
It is okay to use temporary tables.

To set up the same environment as in CI locally, install the test configurations (they will use a Zookeeper mock implementation and adjust some settings)

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
Tests should be
- be minimal: only create the minimally needed tables, columns and, complexity,
- be fast: not take longer than a few seconds (better: sub-seconds),
- be correct and deterministic: fails if and only if the feature-under-test is not working,
- be isolated/stateless: don't rely on environment and timing
- be exhaustive: cover corner cases like zeros, nulls, empty sets, exceptions (negative tests, use syntax `-- { serverError xyz }` and `-- { clientError xyz }` for that),
- clean up tables at the end of the test (in case of leftovers),
- make sure the other tests don't test the same stuff (i.e. grep first).
:::

### Restricting test runs {#restricting-test-runs}

A test can have zero or more _tags_ specifying restrictions in which contexts the test runs in CI.

For `.sql` tests tags are placed in the first line as a SQL comment:

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

For `.sh` tests tags are written as a comment on the second line:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

List of available tags:

|Tag name | What it does | Usage example |
|---|---|---|
| `disabled`|  Test is not run ||
| `long` | Test's execution time is extended from 1 to 10 minutes ||
| `deadlock` | Test is run in a loop for a long time ||
| `race` | Same as `deadlock`. Prefer `deadlock` ||
| `shard` | Server is required to listen to `127.0.0.*` ||
| `distributed` | Same as `shard`. Prefer `shard` ||
| `global` | Same as `shard`. Prefer `shard` ||
| `zookeeper` | Test requires Zookeeper or ClickHouse Keeper to run | Test uses `ReplicatedMergeTree` |
| `replica` | Same as `zookeeper`. Prefer `zookeeper` ||
| `no-fasttest`|  Test is not run under [Fast test](continuous-integration.md#fast-test) | Test uses `MySQL` table engine which is disabled in Fast test|
| `fasttest-only`|  Test is only run under [Fast test](continuous-integration.md#fast-test) ||
| `no-[asan, tsan, msan, ubsan]` | Disables tests in build with [sanitizers](#sanitizers) | Test is run under QEMU which doesn't work with sanitizers |
| `no-replicated-database` |||
| `no-ordinary-database` |||
| `no-parallel` | Disables running other tests in parallel with this one | Test reads from `system` tables and invariants may be broken|
| `no-parallel-replicas` |||
| `no-debug` |||
| `no-stress` |||
| `no-polymorphic-parts` |||
| `no-random-settings` |||
| `no-random-merge-tree-settings` |||
| `no-backward-compatibility-check` |||
| `no-cpu-x86_64` |||
| `no-cpu-aarch64` |||
| `no-cpu-ppc64le` |||
| `no-s3-storage` |||

In addition to above settings, you can use `USE_*` flags from `system.build_options` to define usage of particular ClickHouse features.
For example, if your test uses a MySQL table, you should add a tag `use-mysql`.

### Specifying limits for random settings {#specifying-limits-for-random-settings}

A test can specify minimum and maximum allowed values for settings that can be randomized during test run.

For `.sh` tests limits are written as a comment on the line next to tags or on the second line if no tags are specified:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

For `.sql` tests tags are placed as a SQL comment in the line next to tags or in the first line:

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

If you need to specify only one limit, you can use `None` for another one.

### Choosing the Test Name {#choosing-the-test-name}

The name of the test starts with a five-digit prefix followed by a descriptive name, such as `00422_hash_function_constexpr.sql`.
To choose the prefix, find the largest prefix already present in the directory, and increment it by one.

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

In the meantime, some other tests might be added with the same numeric prefix, but this is OK and does not lead to any problems, you don't have to change it later.

### Checking for an Error that Must Occur {#checking-for-an-error-that-must-occur}

Sometimes you want to test that a server error occurs for an incorrect query. We support special annotations for this in SQL tests, in the following form:

```sql
select x; -- { serverError 49 }
```

This test ensures that the server returns an error with code 49 about unknown column `x`.
If there is no error, or the error is different, the test will fail.
If you want to ensure that an error occurs on the client side, use `clientError` annotation instead.

Do not check for a particular wording of error message, it may change in the future, and the test will needlessly break.
Check only the error code.
If the existing error code is not precise enough for your needs, consider adding a new one.

### Testing a Distributed Query {#testing-a-distributed-query}

If you want to use distributed queries in functional tests, you can leverage `remote` table function with `127.0.0.{1..2}` addresses for the server to query itself; or you can use predefined test clusters in server configuration file like `test_shard_localhost`.
Remember to add the words `shard` or `distributed` to the test name, so that it is run in CI in correct configurations, where the server is configured to support distributed queries.

### Working with Temporary Files {#working-with-temporary-files}

Sometimes in a shell test you may need to create a file on the fly to work with.
Keep in mind that some CI checks run tests in parallel, so if you are creating or removing a temporary file in your script without a unique name this can cause some of the CI checks, such as Flaky, to fail.
To get around this you should use environment variable `$CLICKHOUSE_TEST_UNIQUE_NAME` to give temporary files a name unique to the test that is running.
That way you can be sure that the file you are creating during setup or removing during cleanup is the file only in use by that test and not some other test which is running in parallel.

## Known Bugs {#known-bugs}

If we know some bugs that can be easily reproduced by functional tests, we place prepared functional tests in `tests/queries/bugs` directory.
These tests will be moved to `tests/queries/0_stateless` when bugs are fixed.

## Integration Tests {#integration-tests}

Integration tests allow testing ClickHouse in clustered configuration and ClickHouse interaction with other servers like MySQL, Postgres, MongoDB.
They are useful to emulate network splits, packet drops, etc.
These tests are run under Docker and create multiple containers with various software.

See `tests/integration/README.md` on how to run these tests.

Note that integration of ClickHouse with third-party drivers is not tested.
Also, we currently do not have integration tests with our JDBC and ODBC drivers.

## Unit Tests {#unit-tests}

Unit tests are useful when you want to test not the ClickHouse as a whole, but a single isolated library or class.
You can enable or disable build of tests with `ENABLE_TESTS` CMake option.
Unit tests (and other test programs) are located in `tests` subdirectories across the code.
To run unit tests, type `ninja test`.
Some tests use `gtest`, but some are just programs that return non-zero exit code on test failure.

It's not necessary to have unit tests if the code is already covered by functional tests (and functional tests are usually much more simple to use).

You can run individual gtest checks by calling the executable directly, for example:

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

## Performance Tests {#performance-tests}

Performance tests allow to measure and compare performance of some isolated part of ClickHouse on synthetic queries.
Performance tests are located at `tests/performance/`.
Each test is represented by an `.xml` file with a description of the test case.
Tests are run with `docker/test/performance-comparison` tool . See the readme file for invocation.

Each test run one or multiple queries (possibly with combinations of parameters) in a loop.

If you want to improve performance of ClickHouse in some scenario, and if improvements can be observed on simple queries, it is highly recommended to write a performance test.
Also, it is recommended to write performance tests when you add or modify SQL functions which are relatively isolated and not too obscure.
It always makes sense to use `perf top` or other `perf` tools during your tests.

## Test Tools and Scripts {#test-tools-and-scripts}

Some programs in `tests` directory are not prepared tests, but are test tools.
For example, for `Lexer` there is a tool `src/Parsers/tests/lexer` that just do tokenization of stdin and writes colorized result to stdout.
You can use these kind of tools as a code examples and for exploration and manual testing.

## Miscellaneous Tests {#miscellaneous-tests}

There are tests for machine learned models in `tests/external_models`.
These tests are not updated and must be transferred to integration tests.

There is separate test for quorum inserts.
This test run ClickHouse cluster on separate servers and emulate various failure cases: network split, packet drop (between ClickHouse nodes, between ClickHouse and ZooKeeper, between ClickHouse server and client, etc.), `kill -9`, `kill -STOP` and `kill -CONT` , like [Jepsen](https://aphyr.com/tags/Jepsen). Then the test checks that all acknowledged inserts was written and all rejected inserts was not.

Quorum test was written by separate team before ClickHouse was open-sourced.
This team no longer work with ClickHouse.
Test was accidentally written in Java.
For these reasons, quorum test must be rewritten and moved to integration tests.

## Manual Testing {#manual-testing}

When you develop a new feature, it is reasonable to also test it manually.
You can do it with the following steps:

Build ClickHouse. Run ClickHouse from the terminal: change directory to `programs/clickhouse-server` and run it with `./clickhouse-server`. It will use configuration (`config.xml`, `users.xml` and files within `config.d` and `users.d` directories) from the current directory by default. To connect to ClickHouse server, run `programs/clickhouse-client/clickhouse-client`.

Note that all clickhouse tools (server, client, etc) are just symlinks to a single binary named `clickhouse`.
You can find this binary at `programs/clickhouse`.
All tools can also be invoked as `clickhouse tool` instead of `clickhouse-tool`.

Alternatively you can install ClickHouse package: either stable release from ClickHouse repository or you can build package for yourself with `./release` in ClickHouse sources root.
Then start the server with `sudo clickhouse start` (or stop to stop the server).
Look for logs at `/etc/clickhouse-server/clickhouse-server.log`.

When ClickHouse is already installed on your system, you can build a new `clickhouse` binary and replace the existing binary:

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

Also you can stop system clickhouse-server and run your own with the same configuration but with logging to terminal:

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Example with gdb:

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

If the system clickhouse-server is already running and you do not want to stop it, you can change port numbers in your `config.xml` (or override them in a file in `config.d` directory), provide appropriate data path, and run it.

`clickhouse` binary has almost no dependencies and works across wide range of Linux distributions.
To quick and dirty test your changes on a server, you can simply `scp` your fresh built `clickhouse` binary to your server and then run it as in examples above.

## Build Tests {#build-tests}

Build tests allow to check that build is not broken on various alternative configurations and on some foreign systems.
These tests are automated as well.

Examples:
- cross-compile for Darwin x86_64 (macOS)
- cross-compile for FreeBSD x86_64
- cross-compile for Linux AArch64
- build on Ubuntu with libraries from system packages (discouraged)
- build with shared linking of libraries (discouraged)

For example, build with system packages is bad practice, because we cannot guarantee what exact version of packages a system will have.
But this is really needed by Debian maintainers.
For this reason we at least have to support this variant of build.
Another example: shared linking is a common source of trouble, but it is needed for some enthusiasts.

Though we cannot run all tests on all variant of builds, we want to check at least that various build variants are not broken.
For this purpose we use build tests.

We also test that there are no translation units that are too long to compile or require too much RAM.

We also test that there are no too large stack frames.

## Testing for Protocol Compatibility {#testing-for-protocol-compatibility}

When we extend ClickHouse network protocol, we test manually that old clickhouse-client works with new clickhouse-server and new clickhouse-client works with old clickhouse-server (simply by running binaries from corresponding packages).

We also test some cases automatically with integrational tests:
- if data written by old version of ClickHouse can be successfully read by the new version;
- do distributed queries work in a cluster with different ClickHouse versions.

## Help from the Compiler {#help-from-the-compiler}

Main ClickHouse code (that is located in `src` directory) is built with `-Wall -Wextra -Werror` and with some additional enabled warnings.
Although these options are not enabled for third-party libraries.

Clang has even more useful warnings - you can look for them with `-Weverything` and pick something to default build.

We always use clang to build ClickHouse, both for development and production.
You can build on your own machine with debug mode (to save battery of your laptop), but please note that compiler is able to generate more warnings with `-O3` due to better control flow and inter-procedure analysis.
When building with clang in debug mode, debug version of `libc++` is used that allows to catch more errors at runtime.

## Sanitizers {#sanitizers}

:::note
If the process (ClickHouse server or client) crashes at startup when running it locally, you might need to disable address space layout randomization: `sudo sysctl kernel.randomize_va_space=0`
:::

### Address sanitizer {#address-sanitizer}

We run functional, integration, stress and unit tests under ASan on per-commit basis.

### Thread sanitizer {#thread-sanitizer}

We run functional, integration, stress and unit tests under TSan on per-commit basis.

### Memory sanitizer {#memory-sanitizer}

We run functional, integration, stress and unit tests under MSan on per-commit basis.

### Undefined behaviour sanitizer {#undefined-behaviour-sanitizer}

We run functional, integration, stress and unit tests under UBSan on per-commit basis.
The code of some third-party libraries is not sanitized for UB.

### Valgrind (Memcheck) {#valgrind-memcheck}

We used to run functional tests under Valgrind overnight, but don't do it anymore.
It takes multiple hours.
Currently there is one known false positive in `re2` library, see [this article](https://research.swtch.com/sparse).

## Fuzzing {#fuzzing}

ClickHouse fuzzing is implemented both using [libFuzzer](https://llvm.org/docs/LibFuzzer.html) and random SQL queries.
All the fuzz testing should be performed with sanitizers (Address and Undefined).

LibFuzzer is used for isolated fuzz testing of library code.
Fuzzers are implemented as part of test code and have "_fuzzer" name postfixes.
Fuzzer example can be found at `src/Parsers/fuzzers/lexer_fuzzer.cpp`.
LibFuzzer-specific configs, dictionaries and corpus are stored at `tests/fuzz`.
We encourage you to write fuzz tests for every functionality that handles user input.

Fuzzers are not built by default.
To build fuzzers both `-DENABLE_FUZZING=1` and `-DENABLE_TESTS=1` options should be set.
We recommend to disable Jemalloc while building fuzzers.
Configuration used to integrate ClickHouse fuzzing to
Google OSS-Fuzz can be found at `docker/fuzz`.

We also use simple fuzz test to generate random SQL queries and to check that the server does not die executing them.
You can find it in `00746_sql_fuzzy.pl`.
This test should be run continuously (overnight and longer).

We also use sophisticated AST-based query fuzzer that is able to find huge amount of corner cases.
It does random permutations and substitutions in queries AST.
It remembers AST nodes from previous tests to use them for fuzzing of subsequent tests while processing them in random order.
You can learn more about this fuzzer in [this blog article](https://clickhouse.com/blog/fuzzing-click-house).

## Stress test {#stress-test}

Stress tests are another case of fuzzing.
It runs all functional tests in parallel in random order with a single server.
Results of the tests are not checked.

It is checked that:
- server does not crash, no debug or sanitizer traps are triggered;
- there are no deadlocks;
- the database structure is consistent;
- server can successfully stop after the test and start again without exceptions.

There are five variants (Debug, ASan, TSan, MSan, UBSan).

## Thread Fuzzer {#thread-fuzzer}

Thread Fuzzer (please don't mix up with Thread Sanitizer) is another kind of fuzzing that allows to randomize thread order of execution.
It helps to find even more special cases.

## Security Audit {#security-audit}

Our Security Team did some basic overview of ClickHouse capabilities from the security standpoint.

## Static Analyzers {#static-analyzers}

We run `clang-tidy` on per-commit basis.
`clang-static-analyzer` checks are also enabled.
`clang-tidy` is also used for some style checks.

We have evaluated `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`.
You will find instructions for usage in `tests/instructions/` directory.

If you use `CLion` as an IDE, you can leverage some `clang-tidy` checks out of the box.

We also use `shellcheck` for static analysis of shell scripts.

## Hardening {#hardening}

In debug build we are using custom allocator that does ASLR of user-level allocations.

We also manually protect memory regions that are expected to be readonly after allocation.

In debug build we also involve a customization of libc that ensures that no "harmful" (obsolete, insecure, not thread-safe) functions are called.

Debug assertions are used extensively.

In debug build, if exception with "logical error" code (implies a bug) is being thrown, the program is terminated prematurely.
It allows to use exceptions in release build but make it an assertion in debug build.

Debug version of jemalloc is used for debug builds.
Debug version of libc++ is used for debug builds.

## Runtime Integrity Checks {#runtime-integrity-checks}

Data stored on disk is checksummed.
Data in MergeTree tables is checksummed in three ways simultaneously* (compressed data blocks, uncompressed data blocks, the total checksum across blocks).
Data transferred over network between client and server or between servers is also checksummed.
Replication ensures bit-identical data on replicas.

It is required to protect from faulty hardware (bit rot on storage media, bit flips in RAM on server, bit flips in RAM of network controller, bit flips in RAM of network switch, bit flips in RAM of client, bit flips on the wire).
Note that bit flips are common and likely to occur even for ECC RAM and in presence of TCP checksums (if you manage to run thousands of servers processing petabytes of data each day).
[See the video (russian)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse provides diagnostics that will help ops engineers to find faulty hardware.

\* and it is not slow.

## Code Style {#code-style}

Code style rules are described [here](style.md).

To check for some common style violations, you can use `utils/check-style` script.

To force proper style of your code, you can use `clang-format`.
File `.clang-format` is located at the sources root.
It mostly corresponding with our actual code style.
But it's not recommended to apply `clang-format` to existing files because it makes formatting worse.
You can use `clang-format-diff` tool that you can find in clang source repository.

Alternatively you can try `uncrustify` tool to reformat your code.
Configuration is in `uncrustify.cfg` in the sources root.
It is less tested than `clang-format`.

`CLion` has its own code formatter that has to be tuned for our code style.

We also use `codespell` to find typos in code.
It is automated as well.

## Test Coverage {#test-coverage}

We also track test coverage but only for functional tests and only for clickhouse-server.
It is performed on daily basis.

## Tests for Tests {#tests-for-tests}

There is automated check for flaky tests.
It runs all new tests 100 times (for functional tests) or 10 times (for integration tests).
If at least single time the test failed, it is considered flaky.

## Test Automation {#test-automation}

We run tests with [GitHub Actions](https://github.com/features/actions).

Build jobs and tests are run in Sandbox on per commit basis.
Resulting packages and test results are published in GitHub and can be downloaded by direct links.
Artifacts are stored for several months.
When you send a pull request on GitHub, we tag it as "can be tested" and our CI system will build ClickHouse packages (release, debug, with address sanitizer, etc) for you.
