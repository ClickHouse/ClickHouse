-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM url('http://localhost:8123/', LineAsString, headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', LineAsString, headers('EXACT_HEADER' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', LineAsString, headers('cAsE_INSENSITIVE_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', LineAsString, headers('bad_header_name: test\nexact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', LineAsString, headers('bad_header_value' = 'test\nexact_header: value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', LineAsString, headers('random_header' = 'value')) FORMAT Null;

SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('EXACT_HEADER' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('cAsE_INSENSITIVE_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('bad_header_name: test\nexact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('bad_header_value' = 'test\nexact_header: value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:8123/', LineAsString, headers('random_header' = 'value')) FORMAT Null;

SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('EXACT_HEADER' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('cAsE_INSENSITIVE_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('bad_header_name: test\nexact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('bad_header_value' = 'test\nexact_header: value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:8123/123/4', LineAsString, headers('random_header' = 'value')); -- { serverError S3_ERROR }

SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('EXACT_HEADER' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('cAsE_INSENSITIVE_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('bad_header_name: test\nexact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('bad_header_value' = 'test\nexact_header: value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:8123/123/4', LineAsString, headers('random_header' = 'value')); -- { serverError S3_ERROR }

-- Schema-inference paths must enforce http_forbid_headers BEFORE any network access.
-- These previously bypassed the filter: the request was sent during inference and a forbidden
-- header (e.g. a cloud-metadata auth header) reached the wire, and its response body could leak
-- via a parse error (Regexp), or the filter was never consulted at all (auto-format / DESCRIBE).
SELECT * FROM url('http://localhost:8123/', Regexp, headers('exact_header' = 'value')) SETTINGS format_regexp = 'a'; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', Regexp, headers('cAsE_INSENSITIVE_header' = 'value')) SETTINGS format_regexp = 'a'; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:8123/', headers('bad_header_value' = 'test\nexact_header: value')); -- { serverError BAD_ARGUMENTS }
DESCRIBE url('http://localhost:8123/', headers('exact_header' = 'value')); -- { serverError BAD_ARGUMENTS }
DESCRIBE url('http://localhost:8123/', headers('cAsE_INSENSITIVE_header' = 'value')); -- { serverError BAD_ARGUMENTS }
DESCRIBE url('http://localhost:8123/', Regexp, headers('exact_header' = 'value')) SETTINGS format_regexp = 'a'; -- { serverError BAD_ARGUMENTS }
