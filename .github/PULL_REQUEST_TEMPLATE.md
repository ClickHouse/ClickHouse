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
##### NOTE:
- if your merge the PR with modified CI you **MUST** know what you are doing.
- modifiers can be applied only if set before CI starts
- remove `!` to apply
- return all `!` to restore defaults
```
!#ci_set_<SET_NAME> - to run only preconfigured set of tests, e.g.:
!#ci_set_arm - to run only integration tests on ARM
!#ci_set_integration - to run only integration tests on AMD
!#ci_set_analyzer - to run only tests for analyzer
NOTE: you can configure your own ci set
```
```
!#job_<JOB NAME> - to run only specified job, e.g.:
!#job_stateless_tests_release
!#job_package_debug
!#job_style_check
!#job_integration_tests_asan
```
```
!#batch_2 - to run only 2nd batch for all multi-batch jobs
!#btach_1_2_3 - to run only 1, 2, 3rd batch for all multi-batch jobs
```
```
!#no_merge_commit - to disable merge commit (no merge from master)
!#do_not_test - to disable whole CI (except style check)
```
