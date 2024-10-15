---
slug: /en/development/continuous-integration
sidebar_position: 62
sidebar_label: Continuous Integration Checks
title: Continuous Integration Checks
description: When you submit a pull request, some automated checks are ran for your code by the ClickHouse continuous integration (CI) system
---

When you submit a pull request, some automated checks are ran for your code by
the ClickHouse [continuous integration (CI) system](tests.md#test-automation).
This happens after a repository maintainer (someone from ClickHouse team) has
screened your code and added the `can be tested` label to your pull request.
The results of the checks are listed on the GitHub pull request page as
described in the [GitHub checks
documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
If a check is failing, you might be required to fix it. This page gives an
overview of checks you may encounter, and what you can do to fix them.

If it looks like the check failure is not related to your changes, it may be
some transient failure or an infrastructure problem. Push an empty commit to
the pull request to restart the CI checks:
```
git reset
git commit --allow-empty
git push
```

If you are not sure what to do, ask a maintainer for help.


## Merge With Master

Verifies that the PR can be merged to master. If not, it will fail with the
message `Cannot fetch mergecommit`. To fix this check, resolve the conflict as
described in the [GitHub
documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github),
or merge the `master` branch to your pull request branch using git.


## Docs check

Tries to build the ClickHouse documentation website. It can fail if you changed
something in the documentation. Most probable reason is that some cross-link in
the documentation is wrong. Go to the check report and look for `ERROR` and `WARNING` messages.


## Description Check

Check that the description of your pull request conforms to the template
[PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
You have to specify a changelog category for your change (e.g., Bug Fix), and
write a user-readable message describing the change for [CHANGELOG.md](../whats-new/changelog/index.md)


## Push To DockerHub

Builds docker images used for build and tests, then pushes them to DockerHub.


## Marker Check

This check means that the CI system started to process the pull request. When it has 'pending' status, it means that not all checks have been started yet. After all checks have been started, it changes status to 'success'.


## Style Check

Performs some simple regex-based checks of code style, using the [`utils/check-style/check-style`](https://github.com/ClickHouse/ClickHouse/blob/master/utils/check-style/check-style) binary (note that it can be run locally).
If it fails, fix the style errors following the [code style guide](style.md).

#### Running style check locally:
```sh
mkdir -p /tmp/test_output
# running all checks
python3 tests/ci/style_check.py --no-push

# run specified check script (e.g.: ./check-mypy)
docker run --rm --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output -u $(id -u ${USER}):$(id -g ${USER}) --cap-add=SYS_PTRACE --entrypoint= -w/ClickHouse/utils/check-style clickhouse/style-test ./check-mypy

# find all style check scripts under the directory:
cd ./utils/check-style

# Check duplicate includes
./check-duplicate-includes.sh

# Check c++ formatting
./check-style

# Check python formatting with black
./check-black

# Check python type hinting with mypy
./check-mypy

# Check python with flake8
./check-flake8

# Check code with codespell
./check-typos

# Check docs spelling
./check-doc-aspell

# Check whitespaces
./check-whitespaces

# Check github actions workflows
./check-workflows

# Check submodules
./check-submodules

# Check shell scripts with shellcheck
./shellcheck-run.sh
```

## Fast Test
Normally this is the first check that is ran for a PR. It builds ClickHouse and
runs most of [stateless functional tests](tests.md#functional-tests), omitting
some. If it fails, further checks are not started until it is fixed. Look at
the report to see which tests fail, then reproduce the failure locally as
described [here](tests.md#functional-test-locally).

#### Running Fast Test locally:
```sh
mkdir -p /tmp/test_output
mkdir -p /tmp/fasttest-workspace
cd ClickHouse
# this docker command performs minimal ClickHouse build and run FastTests against it
docker run --rm --cap-add=SYS_PTRACE -u $(id -u ${USER}):$(id -g ${USER})  --network=host -e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output -e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE -e stage=clone_submodules --volume=/tmp/fasttest-workspace:/fasttest-workspace --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output clickhouse/fasttest
```


#### Status Page Files
- `runlog.out.log` is the general log that includes all other logs.
- `test_log.txt`
- `submodule_log.txt` contains the messages about cloning and checkouting needed submodules.
- `stderr.log`
- `stdout.log`
- `clickhouse-server.log`
- `clone_log.txt`
- `install_log.txt`
- `clickhouse-server.err.log`
- `build_log.txt`
- `cmake_log.txt` contains messages about the C/C++ and Linux flags check.

#### Status Page Columns

- *Test name* contains the name of the test (without the path e.g. all types of tests will be stripped to the name).
- *Test status* -- one of _Skipped_, _Success_, or _Fail_.
- *Test time, sec.* -- empty on this test.


## Build Check {#build-check}

Builds ClickHouse in various configurations for use in further steps. You have to fix the builds that fail. Build logs often has enough information to fix the error, but you might have to reproduce the failure locally. The `cmake` options can be found in the build log, grepping for `cmake`. Use these options and follow the [general build process](../development/build.md).

### Report Details

- **Compiler**: `clang-18`, optionally with the name of a target platform
- **Build type**: `Debug` or `RelWithDebInfo` (cmake).
- **Sanitizer**: `none` (without sanitizers), `address` (ASan), `memory` (MSan), `undefined` (UBSan), or `thread` (TSan).
- **Status**: `success` or `fail`
- **Build log**: link to the building and files copying log, useful when build failed.
- **Build time**.
- **Artifacts**: build result files (with `XXX` being the server version e.g. `20.8.1.4344`).
  - `clickhouse-client_XXX_amd64.deb`
  - `clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
  - `clickhouse-common-staticXXX_amd64.deb`
  - `clickhouse-server_XXX_amd64.deb`
  - `clickhouse`: Main built binary.
  - `clickhouse-odbc-bridge`
  - `unit_tests_dbms`: GoogleTest binary with ClickHouse unit tests.
  - `performance.tar.zst`: Special package for performance tests.


## Special Build Check
Performs static analysis and code style checks using `clang-tidy`. The report is similar to the [build check](#build-check). Fix the errors found in the build log.

#### Running clang-tidy locally:
There is a convenience `packager` script that runs the clang-tidy build in docker
```sh
mkdir build_tidy
./docker/packager/packager --output-dir=./build_tidy --package-type=binary --compiler=clang-18 --debug-build --clang-tidy
```


## Functional Stateless Tests
Runs [stateless functional tests](tests.md#functional-tests) for ClickHouse
binaries built in various configurations -- release, debug, with sanitizers,
etc. Look at the report to see which tests fail, then reproduce the failure
locally as described [here](tests.md#functional-test-locally). Note that you
have to use the correct build configuration to reproduce -- a test might fail
under AddressSanitizer but pass in Debug. Download the binary from [CI build
checks page](../development/build.md#you-dont-have-to-build-clickhouse), or build it locally.


## Functional Stateful Tests
Runs [stateful functional tests](tests.md#functional-tests). Treat them in the same way as the functional stateless tests. The difference is that they require `hits` and `visits` tables from the [clickstream dataset](../getting-started/example-datasets/metrica.md) to run.


## Integration Tests
Runs [integration tests](tests.md#integration-tests).


## Bugfix validate check
Checks that either a new test (functional or integration) or there some changed tests that fail with the binary built on master branch. This check is triggered when pull request has "pr-bugfix" label.


## Stress Test
Runs stateless functional tests concurrently from several clients to detect
concurrency-related errors. If it fails:

    * Fix all other test failures first;
    * Look at the report to find the server logs and check them for possible causes
      of error.


## Compatibility Check
Checks that `clickhouse` binary runs on distributions with old libc versions. If it fails, ask a maintainer for help.


## AST Fuzzer
Runs randomly generated queries to catch program errors. If it fails, ask a maintainer for help.


## Performance Tests
Measure changes in query performance. This is the longest check that takes just below 6 hours to run. The performance test report is described in detail [here](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report).
