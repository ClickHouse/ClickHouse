# Lance read-path analysis

## Scope

This document analyzes the current read-only query path for `LanceLocal`, `lanceLocal`, `LanceS3`, and `lanceS3`.

The implementation already provides:

- schema inference and explicit schema validation;
- query-level Lance snapshot pinning;
- projection pushdown;
- basic predicate pushdown;
- filtered and unfiltered `count` pushdown;
- Arrow-based data transfer from the Lance SDK to ClickHouse.

It is a functional read-only MVP, but there is still substantial room to reduce query startup latency and improve scan throughput.

## Current query path

The main path is:

```text
StorageObjectStorage metadata refresh
  -> Lance snapshot and schema resolution
  -> one synthetic dataset object
  -> one LanceReadSource
  -> Rust Lance scanner
  -> Arrow C Data Interface
  -> arrow::Table
  -> ClickHouse Chunk
```

The snapshot is pinned before execution, so schema resolution and data scanning use the same Lance version.

## Reuse the dataset and Tokio runtime

A normal table-engine query can currently perform the following operations:

1. `getTableStateSnapshot` calls `Dataset::open`.
2. `buildStorageMetadataFromState` calls `Dataset::open` again.
3. `LanceReadSource` calls `Dataset::open` when producing its first chunk.
4. Planning the scan creates another Tokio `Runtime`.

For a table function, schema inference can add another dataset open.

For S3 datasets, every dataset open can recreate the Rust object store and reload Lance metadata. This is likely to be a significant part of the latency of short queries.

Introduce a query-scoped Lance session that:

- opens the dataset once;
- owns or shares one `Arc<Runtime>`;
- resolves the snapshot and schema;
- creates the count or scan operation from the same dataset handle;
- keeps the pinned snapshot explicit.

Any cross-query cache must be keyed by the dataset URI, effective storage configuration, credentials identity, and snapshot version. It must not hide errors or return a stale snapshot.

## Increase ClickHouse-side scan parallelism

`LanceDatasetIterator` currently returns one synthetic object for an entire dataset. `ReadFromObjectStorageStep` therefore reduces `num_streams` to one.

The Lance SDK may still read fragments concurrently internally, but the following work remains serialized through one ClickHouse source:

- fetching the next Rust batch;
- importing the batch through FFI;
- converting Arrow columns to ClickHouse columns;
- emitting ClickHouse chunks.

Possible approaches include:

- exposing Lance fragments or scan ranges as iterator entries;
- creating multiple Lance scan streams for one pinned snapshot;
- partitioning fragment work among multiple `LanceReadSource` instances;
- preserving a single-stream path when ordering is required.

Measure the Lance SDK's existing internal concurrency before changing this path. Multiple ClickHouse streams will not help if S3 bandwidth is already saturated, but they can improve Arrow-to-ClickHouse conversion throughput.

## Push down supported parts of `AND`

Predicate extraction is currently all-or-nothing. If any child of `AND` cannot be translated, the complete predicate is left to ClickHouse.

For example:

```sql
SELECT *
FROM lanceS3(...)
WHERE id = 10 AND lower(name) = 'alice';
```

The comparison on `id` can be pushed down even though `lower` is not supported. This is safe because ClickHouse still evaluates the complete filter after reading.

Recommended behavior:

- extract every supported conjunct from `AND`;
- retain the complete ClickHouse filter for final evaluation;
- require every branch of `OR` to be supported;
- add supported expressions such as `notIn`, prefix matching, and other Lance-compatible predicates incrementally;
- cap the size of pushed `IN` sets to avoid building and parsing very large predicate strings;
- consider a typed expression FFI instead of serializing expressions into SQL text.

## Push down `LIMIT`

`Lance::ScanDescription` currently contains the snapshot, projection, predicate, batch size, and count flag, but no limit.

As a result:

```sql
SELECT id
FROM lanceS3(...)
LIMIT 1;
```

can still read a full Lance batch. Propagating a safe query limit into the Lance scanner would reduce work for interactive and existence-check queries.

The limit must not be pushed below operations that can change which rows are returned, such as an unsupported filter, aggregation, or ordering that is not provided by Lance.

## Optimize virtual-only reads

When a query requests only virtual columns, the physical projection is empty. The Rust scanner currently calls `project` only when the projection list is non-empty, so an empty physical projection can become a scan of all physical columns.

For example:

```sql
SELECT _data_lake_snapshot_version
FROM lanceS3(...)
LIMIT 10;
```

Possible optimizations:

- use filtered `countRows` and generate constant virtual-column chunks;
- request a zero-column scan if the Lance SDK supports it;
- otherwise scan the cheapest physical column only;
- combine this path with limit pushdown.

## Remove Arrow conversion layers

Every batch currently passes through:

1. a Rust `RecordBatch`;
2. the Arrow C Data Interface;
3. a C++ `RecordBatch`;
4. a single-batch `arrow::Table`;
5. `ChunkedArray` and column-name maps;
6. a ClickHouse `Chunk`.

The Arrow buffers cross FFI without a data copy, but schema import, wrapper allocation, table construction, hash-map construction, and ClickHouse column conversion still happen for every batch.

Potential improvements:

- add a direct `RecordBatch` to ClickHouse `Chunk` conversion;
- use `ArrowArrayStream` to transfer the stream schema once;
- reuse column-position mappings across batches;
- keep correctness validation for Arrow validity bitmaps.

This optimization is likely to matter most for `LanceLocal`, cached S3 reads, narrow tables, and queries whose storage latency is small.

## Integrate the S3 read path with ClickHouse

The Rust layer creates its own `AmazonS3Builder` and object store. Lance data reads therefore bypass parts of the ClickHouse object-storage infrastructure, including:

- filesystem cache;
- ClickHouse object-storage `ProfileEvents`;
- unified request throttling and scheduling;
- ClickHouse retry and timeout configuration;
- standard read-bytes and progress reporting.

Short-term improvements:

- cache the Rust object store or client within a safe session scope;
- expose Lance request, byte, row, fragment, and batch counters;
- align retry and timeout behavior with ClickHouse settings.

A longer-term option is an object-store adapter backed by ClickHouse `IObjectStorage`. This is a larger integration because it crosses the C++/Rust boundary and must preserve asynchronous I/O behavior.

## Improve `count` planning

`LanceMetadata::totalRows` exists, but `LanceMetadata` does not advertise static `supportsTotalRows` support. The planner-level trivial-count path therefore does not use it.

The custom read source still performs count pushdown, including filtered count pushdown, but advertising snapshot-safe total-row support could avoid constructing the normal read pipeline for trivial `count` queries.

The implementation should reuse the query's pinned dataset and snapshot rather than opening the latest dataset independently.

## Add cancellation

`LanceReadSource` synchronously waits for the Rust stream to return the next batch. There is no explicit FFI cancellation token.

When a query is cancelled during a slow S3 request, ClickHouse may need to wait until the Rust future completes before the source can be destroyed.

Add cancellation propagation from the ClickHouse processor to the Lance scan. Cancellation should stop pending I/O and release the scan without publishing partial state or substituting fallback results.

## Add observability and performance tests

The current SQL and integration tests primarily assert query results. They do not prove that projection, predicate, or count pushdown reduced the physical work.

Add counters such as:

- Lance dataset opens;
- Lance manifest loads;
- Lance fragments scanned and skipped;
- Lance batches and rows returned;
- projected physical columns;
- bytes read from local storage and S3;
- time spent opening datasets, planning scans, waiting for batches, and converting Arrow data.

Performance coverage should include:

- local and S3 datasets;
- cold and warm reads;
- narrow and wide schemas;
- selective and non-selective predicates;
- virtual-only queries;
- `count` and filtered `count`;
- small and large limits;
- single-query throughput and concurrent queries.

The existing functional tests should also assert selected counters so pushdown cannot silently regress while returning correct results.

## Recommended implementation order

1. Add read-path counters and a repeatable local/S3 benchmark.
2. Reuse the dataset handle and Tokio runtime within a query.
3. Push down supported `AND` conjuncts, virtual-only counts, and safe limits.
4. Add multiple scan sources when measurements show a single ClickHouse source is limiting throughput.
5. Convert `RecordBatch` directly to ClickHouse `Chunk`.
6. Integrate Lance S3 reads with ClickHouse caching, scheduling, and cancellation.

The first three items have clear value and relatively contained scope. The later items should be guided by the new counters and benchmarks.
