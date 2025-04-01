---
description: 'Overview of the ClickHouse continuous integration system'
sidebar_label: 'Continuous Integration (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'Continuous Integration (CI)'
---

# Continuous Integration (CI)

When you submit a pull request, some automated checks are ran for your code by the ClickHouse [continuous integration (CI) system](tests.md#test-automation).
This happens after a repository maintainer (someone from ClickHouse team) has screened your code and added the `can be tested` label to your pull request.
The results of the checks are listed on the GitHub pull request page as described in the [GitHub checks documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
If a check is failing, you might be required to fix it.
This page gives an overview of checks you may encounter, and what you can do to fix them.

If it looks like the check failure is not related to your changes, it may be some transient failure or an infrastructure problem.
Push an empty commit to the pull request to restart the CI checks:

```shell
git reset
git commit --allow-empty
git push
```

If you are not sure what to do, ask a maintainer for help.

## Merge with Master {#merge-with-master}

Verifies that the PR can be merged to master.
If not, it will fail with a message `Cannot fetch mergecommit`.
To fix this check, resolve the conflict as described in the [GitHub documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github), or merge the `master` branch to your pull request branch using git.

## Docs check {#docs-check}

Tries to build the ClickHouse documentation website.
It can fail if you changed something in the documentation.
Most probable reason is that some cross-link in the documentation is wrong.
Go to the check report and look for `ERROR` and `WARNING` messages.

## Description Check {#description-check}

Check that the description of your pull request conforms to the template [PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
You have to specify a changelog category for your change (e.g., Bug Fix), and write a user-readable message describing the change for [CHANGELOG.md](../whats-new/changelog/index.md)

## Push To DockerHub {#push-to-dockerhub}

Builds docker images used for build and tests, then pushes them to DockerHub.

## Marker Check {#marker-check}

This check means that the CI system started to process the pull request.
When it has 'pending' status, it means that not all checks have been started yet.
After all checks have been started, it changes status to 'success'.

## Style Check {#style-check}

Performs various style checks on the code base.

Basic checks in the Style Check job:

##### cpp
Performs simple regex-based code style checks using the [`ci/jobs/scripts/check_style/check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh) script (which can also be run locally).  
If it fails, fix the style issues according to the [code style guide](style.md).

##### codespell, aspell
Check for grammatical mistakes and typos.

##### mypy
Performs static type checking for Python code.

### Running the Style Check job locally {#running-style-check-locally}

The entire _Style Check_ job can be run locally in a Docker container with:

```sh
python -m ci.praktika run "Style check"
```

To run a specific check (e.g., _cpp_ check):
```sh
python -m ci.praktika run "Style check" --test cpp
```

These commands pull the `clickhouse/style-test` Docker image and run the job in a containerized environment.
No dependencies other than Python 3 and Docker are required.

## Fast Test {#fast-test}

Normally this is the first check that is run for a PR.
It builds ClickHouse and runs most of [stateless functional tests](tests.md#functional-tests), omitting some.
If it fails, further checks are not started until it is fixed.
Look at the report to see which tests fail, then reproduce the failure locally as described [here](/development/tests#running-a-test-locally).

#### Running Fast Test locally: {#running-fast-test-locally}

```sh
python -m ci.praktika run "Fast test" [--test some_test_name]
```

These commands pull the `clickhouse/fast-test` Docker image and run the job in a containerized environment.
No dependencies other than Python 3 and Docker are required.

## Build Check {#build-check}

Builds ClickHouse in various configurations for use in further steps.
You have to fix the builds that fail.
Build logs often have enough information to fix the error, but you might have to reproduce the failure locally.
The `cmake` options can be found in the build log, grep for `cmake`.
Use these options and follow the [general build process](../development/build.md).

### Report Details {#report-details}

- **Compiler**: `clang-19`, optionally with the name of a target platform
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


## Special Build Check {#special-build-check}
Performs static analysis and code style checks using `clang-tidy`. The report is similar to the [build check](#build-check). Fix the errors found in the build log.

#### Running clang-tidy locally: {#running-clang-tidy-locally}

There is a convenience `packager` script that runs the clang-tidy build in docker
```sh
mkdir build_tidy
./docker/packager/packager --output-dir=./build_tidy --package-type=binary --compiler=clang-19 --debug-build --clang-tidy
```

## Functional Stateless Tests {#functional-stateless-tests}
Runs [stateless functional tests](tests.md#functional-tests) for ClickHouse binaries built in various configurations -- release, debug, with sanitizers, etc.
Look at the report to see which tests fail, then reproduce the failure locally as described [here](/development/tests#functional-tests).
Note that you have to use the correct build configuration to reproduce -- a test might fail under AddressSanitizer but pass in Debug.
Download the binary from [CI build checks page](/install#install-a-ci-generated-binary), or build it locally.

## Functional Stateful Tests {#functional-stateful-tests}

Runs [stateful functional tests](tests.md#functional-tests).
Treat them in the same way as the functional stateless tests.
The difference is that they require `hits` and `visits` tables from the [clickstream dataset](../getting-started/example-datasets/metrica.md) to run.

## Integration Tests {#integration-tests}
Runs [integration tests](tests.md#integration-tests).

## Bugfix validate check {#bugfix-validate-check}

Checks that either a new test (functional or integration) or there some changed tests that fail with the binary built on master branch.
This check is triggered when pull request has "pr-bugfix" label.

## Stress Test {#stress-test}
Runs stateless functional tests concurrently from several clients to detect concurrency-related errors. If it fails:

    * Fix all other test failures first;
    * Look at the report to find the server logs and check them for possible causes
      of error.

## Compatibility Check {#compatibility-check}

Checks that `clickhouse` binary runs on distributions with old libc versions.
If it fails, ask a maintainer for help.

## AST Fuzzer {#ast-fuzzer}
Runs randomly generated queries to catch program errors.
If it fails, ask a maintainer for help.


## Performance Tests {#performance-tests}
Measure changes in query performance.
This is the longest check that takes just below 6 hours to run.
The performance test report is described in detail [here](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report).
