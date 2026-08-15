-- The eager `_path` / `_file` pruning for `file()` and `url()` runs while the
-- pipeline is being built. A `GLOBAL IN` set can only be created when the
-- pipeline runs, so the iterator has to defer the pruning and apply it before
-- trying to open the excluded input.
SELECT * FROM file('04907_file_path_filter_global_in_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

SELECT * FROM url('http://127.0.0.1:1/04907_url_path_filter_global_in_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');
