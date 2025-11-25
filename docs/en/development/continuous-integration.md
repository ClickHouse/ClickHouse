---
description: 'Overview of the ClickHouse continuous integration system'
sidebar_label: 'Continuous Integration (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'Continuous Integration (CI)'
doc_type: 'reference'
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

## Merge with master {#merge-with-master}

Verifies that the PR can be merged to master.
If not, it will fail with a message `Cannot fetch mergecommit`.
To fix this check, resolve the conflict as described in the [GitHub documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github), or merge the `master` branch to your pull request branch using git.

## Docs check {#docs-check}

Tries to build the ClickHouse documentation website.
It can fail if you changed something in the documentation.
Most probable reason is that some cross-link in the documentation is wrong.
Go to the check report and look for `ERROR` and `WARNING` messages.

## Description check {#description-check}

Check that the description of your pull request conforms to the template [PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
You have to specify a changelog category for your change (e.g., Bug Fix), and write a user-readable message describing the change for [CHANGELOG.md](../whats-new/changelog/index.md)

## Docker image {#docker-image}

Builds the ClickHouse server and keeper Docker images to verify that they build correctly.

### Official docker library tests {#official-docker-library-tests}

Runs the tests from the [official Docker library](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files) to verify that the `clickhouse/clickhouse-server` Docker image works correctly.

To add new tests, create a directory `ci/jobs/scripts/docker_server/tests/$test_name` and the script `run.sh` there.

Additional details about the tests can be found in the [CI jobs scripts documentation](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server).

## Marker check {#marker-check}

This check means that the CI system started to process the pull request.
When it has 'pending' status, it means that not all checks have been started yet.
After all checks have been started, it changes status to 'success'.

## Style check {#style-check}

Performs various style checks on the code base.

Basic checks in the Style Check job:

##### cpp {#cpp}
Performs simple regex-based code style checks using the [`ci/jobs/scripts/check_style/check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh) script (which can also be run locally).  
If it fails, fix the style issues according to the [code style guide](style.md).

##### codespell, aspell {#codespell}
Check for grammatical mistakes and typos.

##### mypy {#mypy}
Performs static type checking for Python code.

### Running the style check job locally {#running-style-check-locally}

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

## Fast test {#fast-test}

Normally this is the first check that is run for a PR.
It builds ClickHouse and runs most of [stateless functional tests](tests.md#functional-tests), omitting some.
If it fails, further checks are not started until it is fixed.
Look at the report to see which tests fail, then reproduce the failure locally as described [here](/development/tests#running-a-test-locally).

#### Running fast test locally: {#running-fast-test-locally}

```sh
python -m ci.praktika run "Fast test" [--test some_test_name]
```

These commands pull the `clickhouse/fast-test` Docker image and run the job in a containerized environment.
No dependencies other than Python 3 and Docker are required.

## Build check {#build-check}

Builds ClickHouse in various configurations for use in further steps.

### Running Builds Locally {#running-builds-locally}

The build can be run locally in a CI-like environment using:

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

No dependencies other than Python 3 and Docker are required.

#### Available Build Jobs {#available-build-jobs}

The build job names are exactly as they appear in the CI Report:

**AMD64 Builds:**
- `Build (amd_debug)` - Debug build with symbols
- `Build (amd_release)` - Optimized release build
- `Build (amd_asan)` - Address Sanitizer build
- `Build (amd_tsan)` - Thread Sanitizer build
- `Build (amd_msan)` - Memory Sanitizer build
- `Build (amd_ubsan)` - Undefined Behavior Sanitizer build
- `Build (amd_binary)` - Quick release build without Thin LTO 
- `Build (amd_compat)` - Compatibility build for older systems
- `Build (amd_musl)` - Build with musl libc
- `Build (amd_darwin)` - macOS build
- `Build (amd_freebsd)` - FreeBSD build

**ARM64 Builds:**
- `Build (arm_release)` - ARM64 optimized release build
- `Build (arm_asan)` - ARM64 Address Sanitizer build
- `Build (arm_coverage)` - ARM64 build with coverage instrumentation
- `Build (arm_binary)` - ARM64 Quick release build without Thin LTO
- `Build (arm_darwin)` - macOS ARM64 build
- `Build (arm_v80compat)` - ARMv8.0 compatibility build

**Other Architectures:**
- `Build (ppc64le)` - PowerPC 64-bit Little Endian
- `Build (riscv64)` - RISC-V 64-bit
- `Build (s390x)` - IBM System/390 64-bit
- `Build (loongarch64)` - LoongArch 64-bit

If the job succeeds, build results will be available in the `<repo_root>/ci/tmp/build` directory.

**Note:** For builds not in the "Other Architectures" category (which use cross-compilation), your local machine architecture must match the build type to produce the build as requested by `BUILD_JOB_NAME`.

#### Example {#example-run-local}

To run a local debug build:

```bash
python -m ci.praktika run "Build (amd_debug)"
```

If the above approach does not work for you, use the cmake options from the build log and follow the [general build process](../development/build.md).
## Functional stateless tests {#functional-stateless-tests}

Runs [stateless functional tests](tests.md#functional-tests) for ClickHouse binaries built in various configurations -- release, debug, with sanitizers, etc.
Look at the report to see which tests fail, then reproduce the failure locally as described [here](/development/tests#functional-tests).
Note that you have to use the correct build configuration to reproduce -- a test might fail under AddressSanitizer but pass in Debug.
Download the binary from [CI build checks page](/install/advanced), or build it locally.

## Integration tests {#integration-tests}

Runs [integration tests](tests.md#integration-tests).

## Bugfix validate check {#bugfix-validate-check}

Checks that either a new test (functional or integration) or there some changed tests that fail with the binary built on master branch.
This check is triggered when pull request has "pr-bugfix" label.

## Stress test {#stress-test}

Runs stateless functional tests concurrently from several clients to detect concurrency-related errors. If it fails:

    * Fix all other test failures first;
    * Look at the report to find the server logs and check them for possible causes
      of error.

## Compatibility check {#compatibility-check}

Checks that `clickhouse` binary runs on distributions with old libc versions.
If it fails, ask a maintainer for help.

## AST fuzzer {#ast-fuzzer}

Runs randomly generated queries to catch program errors.
If it fails, ask a maintainer for help.

## Performance tests {#performance-tests}

Measure changes in query performance.
This is the longest check that takes just below 6 hours to run.
The performance test report is described in detail [here](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report).
