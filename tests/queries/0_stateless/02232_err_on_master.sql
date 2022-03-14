-- Tags: no-fasttest
SELECT value not like '22%' FROM system.build_options WHERE name = 'GIT_BRANCH';
