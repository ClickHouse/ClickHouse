<!---
A technical comment, you are free to remove or leave it as it is when PR is created
The following categories are used in the next scripts, update them accordingly
utils/changelog/changelog.py
tests/ci/cancel_and_rerun_workflow_lambda/app.py
-->
### Changelog category (leave one):
- New Feature
- Experimental Feature
- Improvement
- Performance Improvement
- Backward Incompatible Change
- Build/Testing/Packaging Improvement
- Documentation (changelog entry is not required)
- Critical Bug Fix (crash, LOGICAL_ERROR, data loss, RBAC)
- Bug Fix (user-visible misbehavior in an official stable release)
- CI Fix or Improvement (changelog entry is not required)
- Not for changelog (changelog entry is not required)


### Changelog entry (a user-readable short description of the changes that goes to CHANGELOG.md):
...

### Documentation entry for user-facing changes

- [ ] Documentation is written (mandatory for new features)

<!---
Directly edit documentation source files in the "docs" folder with the same pull-request as code changes

or

Add a user-readable short description of the changes that should be added to docs.clickhouse.com below.

At a minimum, the following information should be added (but add more as needed).
- Motivation: Why is this function, table engine, etc. useful to ClickHouse users?

- Parameters: If the feature being added takes arguments, options or is influenced by settings, please list them below with a brief explanation.

- Example use: A query or command.
-->


> Information about CI checks: https://clickhouse.com/docs/en/development/continuous-integration/

#### CI Settings (Only check the boxes if you know what you are doing):
- [ ] <!---ci_set_required--> Allow: All Required Checks
- [ ] <!---ci_include_stateless--> Allow: Stateless tests
- [ ] <!---ci_include_stateful--> Allow: Stateful tests
- [ ] <!---ci_include_integration--> Allow: Integration Tests
- [ ] <!---ci_include_performance--> Allow: Performance tests
- [ ] <!---ci_set_builds--> Allow: All Builds
- [ ] <!---batch_0_1--> Allow: batch 1, 2 for multi-batch jobs
- [ ] <!---batch_2_3--> Allow: batch 3, 4, 5, 6 for multi-batch jobs
---
- [ ] <!---ci_exclude_style--> Exclude: Style check
- [ ] <!---ci_exclude_fast--> Exclude: Fast test
- [ ] <!---ci_exclude_asan--> Exclude: All with ASAN
- [ ] <!---ci_exclude_tsan|msan|ubsan|coverage--> Exclude: All with TSAN, MSAN, UBSAN, Coverage
- [ ] <!---ci_exclude_aarch64|release|debug--> Exclude: All with aarch64, release, debug
---
- [ ] <!---ci_include_fuzzer--> Run only fuzzers related jobs (libFuzzer fuzzers, AST fuzzers, etc.)
- [ ] <!---ci_exclude_ast--> Exclude: AST fuzzers
---
- [ ] <!---do_not_test--> Do not test
- [ ] <!---woolen_wolfdog--> Woolen Wolfdog
- [ ] <!---upload_all--> Upload binaries for special builds
- [ ] <!---no_merge_commit--> Disable merge-commit
- [ ] <!---no_ci_cache--> Disable CI cache
