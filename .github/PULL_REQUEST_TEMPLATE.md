<!---
A technical comment, you are free to remove or leave it as it is when PR is created
The following categories are used in the next scripts, update them accordingly
utils/changelog/changelog.py
tests/ci/cancel_and_rerun_workflow_lambda/app.py
-->
### Changelog category (leave one):
- New Feature
- Improvement
- Performance Improvement
- Backward Incompatible Change
- Build/Testing/Packaging Improvement
- Documentation (changelog entry is not required)
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

---
### Modify your CI run:
**NOTE:** If your merge the PR with modified CI you **MUST KNOW** what you are doing
**NOTE:** Checked options will be applied if set before CI RunConfig/PrepareRunConfig step

#### Include tests (required builds will be added automatically):
- [ ] <!---ci_include_fast--> Fast test
- [ ] <!---ci_include_integration--> Integration Tests
- [ ] <!---ci_include_stateless--> Stateless tests
- [ ] <!---ci_include_stateful--> Stateful tests
- [ ] <!---ci_include_unit--> Unit tests
- [ ] <!---ci_include_performance--> Performance tests
- [ ] <!---ci_include_asan--> All with ASAN
- [ ] <!---ci_include_tsan--> All with TSAN
- [ ] <!---ci_include_analyzer--> All with Analyzer
- [ ] <!---ci_include_KEYWORD--> Add your option here

#### Exclude tests:
- [ ] <!---ci_exclude_fast--> Fast test
- [ ] <!---ci_exclude_integration--> Integration Tests
- [ ] <!---ci_exclude_stateless--> Stateless tests
- [ ] <!---ci_exclude_stateful--> Stateful tests
- [ ] <!---ci_exclude_performance--> Performance tests
- [ ] <!---ci_exclude_asan--> All with ASAN
- [ ] <!---ci_exclude_tsan--> All with TSAN
- [ ] <!---ci_exclude_msan--> All with MSAN
- [ ] <!---ci_exclude_ubsan--> All with UBSAN
- [ ] <!---ci_exclude_coverage--> All with Coverage
- [ ] <!---ci_exclude_aarch64--> All with Aarch64
- [ ] <!---ci_exclude_KEYWORD--> Add your option here

#### Extra options:
- [ ] <!---do_not_test--> do not test (only style check)
- [ ] <!---no_merge_commit--> disable merge-commit (no merge from master before tests)
- [ ] <!---no_ci_cache--> disable CI cache (job reuse)

#### Only specified batches in multi-batch jobs:
- [ ] <!---batch_0--> 1
- [ ] <!---batch_1--> 2
- [ ] <!---batch_2--> 3
- [ ] <!---batch_3--> 4
