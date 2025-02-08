SELECT normalized_query_hash AS a, normalizedQueryHash(query) AS b, a = b FROM system.processes;
