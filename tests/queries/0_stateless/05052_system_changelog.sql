-- system.changelog is generated from the curated CHANGELOG.md committed into the repository,
-- so the test asserts shape and invariants of the data rather than its contents.

SELECT count() > 100 FROM system.changelog;
SELECT countIf(description = ''), countIf(category = ''), countIf(version.major < 20), countIf(release_date < toDate('2020-01-01')) FROM system.changelog;
SELECT count() > 0 FROM system.changelog WHERE category = 'New Feature';
SELECT count() > 0 FROM system.changelog WHERE notEmpty(authors) AND notEmpty(pull_requests);

-- The version tuple is comparable, so range filters and ordering need no string tricks.
SELECT count() > 0 FROM system.changelog WHERE version >= (26, 1, 0);
SELECT uniqExact(version) >= 1 FROM system.changelog;
SELECT version = tuple(version.major, version.minor, version.patch) FROM system.changelog LIMIT 1;
