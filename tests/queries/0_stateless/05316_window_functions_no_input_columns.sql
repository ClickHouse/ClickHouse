-- A window that reads no column at all still spans the whole partition across many input blocks.
SELECT min(w), max(w), count() FROM (SELECT count() OVER () AS w FROM numbers_mt(100000)) SETTINGS max_block_size = 1000, max_threads = 4;
SELECT min(w), max(w), count() FROM (SELECT sum(1) OVER () AS w FROM numbers_mt(100000)) SETTINGS max_block_size = 1000, max_threads = 4;
SELECT min(r), max(r), min(d), max(d), max(n) FROM (SELECT rank() OVER () AS r, dense_rank() OVER () AS d, row_number() OVER () AS n FROM numbers_mt(100000)) SETTINGS max_block_size = 1000, max_threads = 4;
SELECT count(), min(w), max(w) FROM (SELECT count() OVER () AS w FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 5) SETTINGS max_block_size = 100, max_threads = 16;
