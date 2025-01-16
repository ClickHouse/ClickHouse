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
- Critical Bug Fix (crash, data loss, RBAC) or LOGICAL_ERROR
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

#### CI Settings (Only check the boxes if you know what you are doing)

All builds in Builds_1 and Builds_2 stages are always mandatory
and will run independently of the checks below:
- [ ] <!---ci_include_stateless--> Only: Stateless tests
- [ ] <!---ci_include_stateful--> Only: Stateful tests
- [ ] <!---ci_include_integration--> Only: Integration tests
- [ ] <!---ci_include_performance--> Only: Performance tests
---
- [ ] <!---ci_exclude_style--> Skip: Style check
- [ ] <!---ci_exclude_fast--> Skip: Fast test
---
- [ ] <!---woolen_wolfdog--> Non-blocking CI mode (Resource-intensive. All test jobs execute in parallel).
- [ ] <!---no_merge_commit--> Disable merge-commit (Run CI on branch HEAD instead of merge commit with target branch)
- [ ] <!---no_ci_cache--> Disable CI cache
