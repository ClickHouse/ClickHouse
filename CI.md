## A small note about ClickHouse's CI system.
### CI's checks overview

0. Merge with master
    - Runs only on PRs.
    - Checks if the PR can be successfully merged with the master branch.
    - Not present in the checks list if there are no conflicts.
    - Status if the conflicts are present: `Cannot fetch mergecommit`.
1. [Docs check](#-docs-check)
    - Tries to build the ClickHouse's documentation website.
    - Checks the inner cross-links status.
2. Description check
    - Run only on PRs.
    - Check the conformance of the PR's description with the [PULL_REQUEST_TEMPLATE.md](https://github.com/yandex/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
3. Push to dockerhub
    - Checks differences between the ClickHouse's current DockerHub image.
    - Status on success "nothing to update" or "successfully updated".
4. Marker check
    - Checks that all CI's check have been initiated.
    - Status on success: `All checks were started`.
5. [Style check](#-style-check)
    - Checks the code style according to `utils/check-style/check-style` binary.
    - Status on success: `Style check passed`.
6. [PVS check](#-pvs-check)
    - Static analysis tool, checks code with PVS-studio.
7. [Fast test](#-fast-test)
    - First executable check.
    - Builds a subset of ClickHouse's code (without embedded compiler, ZooKeeper, etc.).
    - Runs a subset of tests (functional, stateless, stateful, and integration ones).
    - Blocks the start of Build check on errors.
    - Intended to detect some errors and limit the CI run time on broken commits.
    - Status on success: `fail: 0, passed: 1944, skipped: 229` (may differ).
8. [Build check](#-build-check)
    - Builds ClickHouse in various configurations for use in further steps.
    - Uses compilers `gcc-9` and `clang-10`.
    - Uses build modes `Debug` and `RelWithDebInfo`.
    - Uses sanitizers: `undefined` (UBSan), `address` (Asan), `memory` (MSan), and `thread` (TSan).
    - Status on success: `20/20 builds are OK` (may vary).
9. Special build check
    - Checks the code with `clang-tidy`.
10. Compatibility check
    - Checks that `clickhouse` binary runs on distributions with old libc versions.
11. AST fuzzer
12. Functional stateful tests
13. Functional stateless tests
14. Integration tests
15. Stress test
16. Split build smoke test
17. Testflows check
18. Performance tests.
    - Intended to measure the queries' relative and absolute performance changes.
    - The longest check: takes about 5 hours to run.

### Docs check
- [Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
- `docs_output.txt` contains the building log. [Successful result example](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)
### Style check
- [Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check.html)
- `output.txt` contains the check resulting errors (invalid tabulation etc), blank page means no errors. [Successful result example](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check/output.txt).

### PVS check
- [Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/pvs_check.html)
- `test_run.txt.out.log` contains the building and analyzing log file. It includes only parsing or not-found errors.
- `HTML report` contains the analysis results. For its description visit PVS's [official site](https://www.viva64.com/en/m/0036/#ID14E9A2B2CD).

### Fast test
[Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/fast_test.html)

#### Status page files
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

#### Status page columns:

- *Test name* contains the name of the test (without the path e.g. all types of tests will be stripped to the name).
- *Test status* -- one of _Skipped_, _Success_, or _Fail_.
- *Test time, sec.* -- empty on this test.

### Build check

[Status page example](https://clickhouse-builds.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/clickhouse_build_check/report.html).

#### Status page columns:

- **Compiler**: `gcc-9` or `clang-10` (or `clang-10-xx` for other architectures e.g. `clang-10-freebsd`).
- **Build type**: `Debug` or `RelWithDebInfo` (cmake).
- **Sanitizer**: `none` (without sanitizers), `address` (ASan), `memory` (MSan), `undefined` (UBSan), or `thread` (TSan).
- **Bundled**: ???
- **Splitted** ???
- **Status**: `success` or `fail`
- **Build log**: link to the building and files copying log, useful when build failed.
- **Build time**.
- **Artifacts**: build result files (with `XXX` being the server version e.g. `20.8.1.4344`).
    - `clickhouse-client_XXX_all.deb`
    - `clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
    - `clickhouse-common-staticXXX_amd64.deb`
    - `clickhouse-server_XXX_all.deb`
    - `clickhouse-test_XXX_all.deb`
    - `clickhouse_XXX_amd64.buildinfo`
    - `clickhouse_XXX_amd64.changes`
    - `clickhouse`: Main built binary.
    - `clickhouse-odbc-bridge`
    - `unit_tests_dbms`: GoogleTest binary with ClickHouse unit tests.
    - `shared_build.tgz`: build with shared libraries.
    - `performance.tgz`: Special ??? build for performance tests.

## Where the tests are run

Intel Gold with 100500ZB RAM.

## QA

> What is a `Task (private network)` item on status pages?

It's a link to the Yandex's internal job system. Yandex guys can see the check's start time and its more verbose status.
