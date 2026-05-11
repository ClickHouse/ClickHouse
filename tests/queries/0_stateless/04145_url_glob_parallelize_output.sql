-- Tags: no-fasttest
-- Test: exercises `ReadFromURL::initializePipeline` with a globbed URL whose glob
-- iterator yields fewer files than `max_threads`, hitting the post-read resize branch
-- with `output_ports < max_num_streams`.
-- Covers: src/Storages/StorageURL.cpp:1362 — the resize uses the saved `max_num_streams`,
-- not the (already-clamped) `num_streams`. PR's own test (02723) only exercises the
-- single-URL no-glob branch where `num_streams` is set to 1 (line 1310). The glob
-- branch that reassigns `num_streams = std::min(num_streams, glob_iterator->size())`
-- (line 1299) is untested by 02723; before the fix, this path would skip the resize.

-- 2 URL options, max_threads=4: glob iterator size = 2, so `num_streams` becomes 2.
-- After the fix the URL source step ends with `Resize 2 → 4`; before the fix it ended
-- with `URL × 2 0 → 1` (no resize because the buggy condition `output_ports < num_streams`
-- evaluated to `2 < 2 = false`).
select match(arrayStringConcat(groupArray(explain), ''), '.*Resize 2 → 4 *URL × 2 0 → 1 *$')
from (explain pipeline
    select x, count() from url('https://example.{com,org}', Parquet, 'x Int64')
    group by x order by count() limit 10
) settings max_threads=4, parallelize_output_from_storages=1;
