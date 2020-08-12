## A small note about ClickHouse's CI system.
### CI's checks overview

0. Merge with master
    - Runs only on PRs.
    - Checks if the PR can be successfully merged with the master branch.
    - Not present in the checks list if there are no conflicts.
    - Status if the conflicts are present: "Cannot fetch mergecommit".
1. [Docs check](#docs-check)
    - Tries to build the ClickHouse's documentation website.
    - Checks the inner cross-links status.
2. Description check
    - Run only on PRs.
    - Check the conformance of the PR's description with the PULL_REQUEST_TEMPLATE.md
3. Push to dockerhub
    - Checks differences between the ClickHouse's current DockerHub image.
    - Status on success "nothing to update" or "successfully updated".
4. Marker check
    - Checks that all CI's check have been initiated.
    - Success status: "All checks were started".
5. [Style check](#style-check)
    - Checks the code style according to `utils/check-style/check-style`.
    - Status on success: "Style check passed".
6. PVS check
    - Static analysis tool, checks code with PVS-studio.
7. Fast test
    - First executable check.
    - Builds a subset of ClickHouse's code (without embedded compiler, ZooKeeper, etc.).
    - Runs a subset of tests (functional, stateless, stateful, and integration ones).
    - Blocks the start of Build check on errors.
    - Intended to detect some errors and limit the CI run time on broken commits.
8. Build check
    - Build ClickHouse with various compilers (gcc-9, clang-10) and various sanitizers:
    undfined (UBSan), address (ASan), memory (MSan), and thread (TSan).
9. Special build check
    - ???
10. Compatibility check
    - Checks that `clickhouse` binary runs on distros with old libc versions.
11. AST fuzzer
12. Functional stateful tests
13. Functional stateless tests
14. Integration tests
15. Stress test
16. Split build smoke test
17. Testflows check
18. Performance tests.

### <a name="docs-check"></a> Docs check
- [Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
- `docs_output.txt` contains the building log.  [Successful result example](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)

### <a name="style-check"></a> Style check
- [Status page example](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check.html)
- `output.txt` contains the check resulting errors (invalid tabulation etc), blank page means no errors. [Succesful
  result example](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check/output.txt)
