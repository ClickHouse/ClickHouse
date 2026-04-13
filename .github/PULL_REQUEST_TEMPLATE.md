### Changelog category (leave one):
- New Feature
- Experimental Feature
- Improvement
- Performance Improvement
- Backward Incompatible Change
- Build/Testing/Packaging Improvement
- Documentation (changelog entry is not required)
- Critical Bug Fix (crash, data loss, RBAC)
- Bug Fix (user-visible misbehavior in an official stable release)
- CI Fix or Improvement (changelog entry is not required)
- Not for changelog (changelog entry is not required)


### Changelog entry (a user-readable short description of the changes that goes to CHANGELOG.md):
...

### Documentation entry for user-facing changes
...

### CI/CD Options
#### Exclude tests:
- [ ] <!---ci_exclude_fast--> Fast test
- [ ] <!---ci_exclude_integration--> Integration Tests
- [ ] <!---ci_exclude_stateless--> Stateless tests
- [ ] <!---ci_exclude_stateful--> Stateful tests
- [ ] <!---ci_exclude_performance--> Performance tests
- [ ] <!---ci_exclude_asan--> All with ASAN
- [x] <!---ci_exclude_tsan--> All with TSAN
- [x] <!---ci_exclude_msan--> All with MSAN
- [x] <!---ci_exclude_ubsan--> All with UBSAN
- [x] <!---ci_exclude_coverage--> All with Coverage
- [ ] <!---ci_exclude_aarch64|arm--> All with Aarch64
- [ ] <!---ci_exclude_regression--> All Regression
- [ ] <!---no_ci_cache--> Disable CI Cache

#### Regression jobs to run:
- [ ] <!---ci_regression_common--> Fast suites (mostly <1h)
- [ ] <!---ci_regression_aggregate_functions--> Aggregate Functions (2h)
- [ ] <!---ci_regression_alter--> Alter (1.5h)
- [ ] <!---ci_regression_benchmark--> Benchmark (30m)
- [ ] <!---ci_regression_clickhouse_keeper--> ClickHouse Keeper (1h)
- [x] <!---ci_regression_iceberg--> Iceberg (2h)
- [ ] <!---ci_regression_ldap--> LDAP (1h)
- [x] <!---ci_regression_parquet--> Parquet (1.5h)
- [ ] <!---ci_regression_rbac--> RBAC (1.5h)
- [ ] <!---ci_regression_ssl_server--> SSL Server (1h)
- [ ] <!---ci_regression_s3--> S3 (2h)
- [x] <!---ci_regression_s3_export--> S3 Export (2h)
- [x] <!---ci_regression_swarms--> Swarms (30m)
- [ ] <!---ci_regression_tiered_storage--> Tiered Storage (2h)
