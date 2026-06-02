# vector_spann (experimental)

Code: `MergeTreeIndexVectorSpann.{h,cpp}`, factory name `vector_spann`, gated by `USE_USEARCH`. Internal notes; user-facing syntax is in `docs/en/engines/table-engines/mergetree-family/annindexes.md`.

Goal: validate MergeTree plumbing for a SPANN-style layout (centroid graph + on-disk posting lists + query-time rerank). This is **not** full SPANN recall/performance yet.

Paper uses SPTAG for centroid indexing; we use USearch because `vector_similarity` already does and centroid count is much smaller than N.

**Not yet implemented:** HBC centroid selection, closure expansion / posting replicas, query-aware dynamic pruning, selective posting-list IO.

## Disk

Same multi-stream idea as `MergeTreeIndexText`:

- Regular stream (`skp_idx_<name>.idx`): file format version, dimensions, centroid count, serialized centroid HNSW (`USearchIndexWithSerialization`), then per-centroid `(offset, length)` into the posting blob.
- Posting stream (`skp_idx_<name>.pl.idx`): suffix `.pl`, extension `.idx`. Actual basename from `getFileName()`; long names may be escaped or hashed like other skip indexes.

Each centroid block: `UInt32 count`, then `count` times `(UInt64 row_id, dim × Float32)`. Full vectors are stored here so `calculateApproximateNearestNeighbors` can refine without reading the indexed array column again (trades space for a simpler query path).

## Build (`getGranuleAndReset`)

1. `selectEvenlySpacedCentroids`: `ceil(n * centroid_ratio)` centroids, clamped to `[1, n]` (default ratio 0.16, inspired by SPANN paper ablation — not equivalent to full SPANN tuning). Indices `(i * n) / num_centroids` for `i = 0..k-1`; deterministic for replicated builds. **Not** HBC.
2. `buildCentroidIndex`: USearch HNSW on centroid vectors; sequential `add` for deterministic graph build.
3. `assignAndMakePostings`: top-1 nearest centroid per row, fill `postings_by_centroid` and offsets. Disk write happens later in `serializeBinaryWithMultipleStreams`.

## Query (`calculateApproximateNearestNeighbors`)

Centroid search with K from `hnsw_candidate_list_size_for_search` (stored as `max_posting_lists`), expansion `default_expansion_search`. Walk matching posting lists, `distanceToQuery`, sort to top-k. Row ids are granule-local; hooks into the same vector-index path as `vector_similarity` via `filterMarksUsingIndex`.

## Deserialize

`deserializeBinaryWithMultipleStreams` **eager-loads all posting lists** for the skip-index granule into memory. Selective IO (read only posting ranges for centroids hit by the query) needs API work: `calculateApproximateNearestNeighbors` receives only `granule`, not `MergeTreeIndexReader` streams.

Gaps vs the SPANN paper (also noted in comments in `MergeTreeIndexVectorSpann.cpp`): no closure replicas, no dynamic pruning, full granule eager load; posting-list size guard in `assignAndMakePostings`.
