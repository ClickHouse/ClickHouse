# vector_spann (experimental)

Code: `MergeTreeIndexVectorSpann.{h,cpp}`, factory name `vector_spann`, gated by `USE_USEARCH`. Not user-facing docs.

Paper uses SPTAG for centroid indexing; we use USearch because `vector_similarity` already does and centroid count is much smaller than N.

## Disk

Same multi-stream idea as `MergeTreeIndexText`:

- Regular stream (`skp_idx_<name>.idx`): file format version, dimensions, centroid count, serialized centroid HNSW (`USearchIndexWithSerialization`), then per-centroid `(offset, length)` into the posting blob.
- Posting stream (`skp_idx_<name>.pl.idx`): suffix `.pl`, extension `.idx`. Actual basename from `getFileName()`; long names may be escaped or hashed like other skip indexes.

Each centroid block: `UInt32 count`, then `count` times `(UInt64 row_id, dim × Float32)`. Full vectors are stored here so `calculateApproximateNearestNeighbors` can refine without reading the indexed array column again (trades space for a simpler query path).

## Build (`getGranuleAndReset`)

1. `selectCentroidsRandom`: `ceil(n * centroid_ratio)` centroids, clamped to `[1, n]` (default ratio 0.16). No HBC yet.
2. `buildCentroidIndex`: USearch HNSW on centroid vectors, parallel via `getBuildVectorSimilarityIndexThreadPool`.
3. `assignAndMakePostings`: nearest centroid per row, fill `postings_by_centroid` and offsets. Disk write happens later in `serializeBinaryWithMultipleStreams`.

## Query (`calculateApproximateNearestNeighbors`)

Centroid search with K from `hnsw_candidate_list_size_for_search` (stored as `max_posting_lists`), expansion `default_expansion_search`. Walk matching posting lists, `distanceToQuery`, sort to top-k. Row ids are granule-local; hooks into the same vector-index path as `vector_similarity` via `filterMarksUsingIndex`.

Gaps vs the SPANN paper here (no closure replicas, full granule eager load): details in comments on `deserializeBinaryWithMultipleStreams`, `calculateApproximateNearestNeighbors`, and the posting-list size guard in `assignAndMakePostings` (`MergeTreeIndexVectorSpann.cpp`).
