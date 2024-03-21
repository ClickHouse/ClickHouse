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
**NOTE:** Set desired options before CI starts or re-push after updates

#### Run only:
- [ ] <!---ci_set_integration--> Integration tests
- [ ] <!---ci_set_arm--> Integration tests (arm64)
- [ ] <!---ci_set_stateless--> Stateless tests (release)
- [ ] <!---ci_set_stateless_asan--> Stateless tests (asan)
- [ ] <!---ci_set_stateful--> Stateful tests (release)
- [ ] <!---ci_set_stateful_asan--> Stateful tests (asan)
- [ ] <!---ci_set_reduced--> No sanitizers
- [ ] <!---ci_set_analyzer--> Tests with analyzer
- [ ] <!---ci_set_fast--> Fast tests
- [ ] <!---job_package_debug--> Only package_debug build
- [ ] <!---PLACE_YOUR_TAG_CONFIGURED_IN_ci_config.py_FILE_HERE--> Add your CI variant description here

#### CI options:
- [ ] <!---do_not_test--> do not test (only style check)
- [ ] <!---no_merge_commit--> disable merge-commit (no merge from master before tests)
- [ ] <!---no_ci_cache--> disable CI cache (job reuse)

#### Only specified batches in multi-batch jobs:
- [ ] <!---batch_0--> 1
- [ ] <!---batch_1--> 2
- [ ] <!---batch_2--> 3
- [ ] <!---batch_3--> 4
