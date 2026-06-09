---
title: "Vector Search PDX Integration"
slug: /development/vector-search-pdx-integration
---

# Vector Search PDX Integration

This document describes how `vector_similarity` works in MergeTree and where the `pdx` method is integrated.

## Execution Path

1. DDL validation is handled by `vectorSimilarityIndexValidator` in `MergeTreeIndexVectorSimilarity.cpp`.
2. Skip-index build runs through:
   - `MergeTreeDataPartWriterOnDisk::initSkipIndices`
   - `MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices`
   - `IMergeTreeIndexAggregator::update` and `getGranuleAndReset`
3. Serialized granules are written into `skp_idx_<index_name>.idx` streams.
4. At read time:
   - `ReadFromMergeTree::buildIndexes` creates index conditions.
   - `MergeTreeDataSelectExecutor::filterMarksUsingIndex` calls `calculateApproximateNearestNeighbors` for vector indexes.
   - Result row offsets are converted to mark ranges and injected into vector-search hints.
5. During row-level read, `MergeTreeRangeReader::fillDistanceColumnAndFilterForVectorSearch` computes `_distance` and final filtering/reranking behavior.

## Method Dispatch (`hnsw` / `pdx`)

`MergeTreeIndexVectorSimilarity` now stores `VectorSimilarityMethod` and dispatches all lifecycle operations by method:

- `hnsw`: existing USearch path.
- `pdx`: PDX path using `PDXIndexF32` from `contrib/pdx`.

Dispatch points:

- `createIndexGranule`
- `createIndexAggregator`
- `createIndexCondition`
- `MergeTreeIndexAggregatorVectorSimilarity::update`
- `MergeTreeIndexConditionVectorSimilarity::calculateApproximateNearestNeighbors`

## Storage Format and Compatibility

`MergeTreeIndexGranuleVectorSimilarity` persistence format is versioned at ClickHouse level.

- Version `1`: legacy format (HNSW-only payload).
- Version `2`: adds explicit method tag (`hnsw`/`pdx`) and keeps dimensions header.

Compatibility rules:

- Existing HNSW parts written with version `1` are still readable.
- New parts are written with version `2`.
- Method mismatch between index metadata and persisted payload fails with a clear error and requires index rebuild.

## PDX Build and Search in Granules

- Build phase (`update`): vectors are collected row-major in `float` buffer.
- Finalization (`getGranuleAndReset`): `PDXIndexF32` is built per granule.
- Search phase (`calculateApproximateNearestNeighbors`):
  - `hnsw_candidate_list_size_for_search` is mapped to PDX `nprobe` (clamped to cluster count).
  - `vector_search_index_fetch_multiplier` and `max_limit_for_vector_search_queries` are reused exactly as for HNSW.
  - Distances/row ids are returned in the same `NearestNeighbours` structure.

## PDX Integration Constraints

- `pdx` is available only when `USE_PDX=1`.
- If ClickHouse is built without PDX support, DDL with `vector_similarity('pdx', ...)` fails with `PDX support is not compiled`.
- MVP scope keeps existing vector-search planner/runtime behavior intact (including current part-level filtering limitations).

## Source Layout

- Integration logic: `src/Storages/MergeTree/MergeTreeIndexVectorSimilarity.{h,cpp}`
- Build wiring:
  - `contrib/pdx-cmake/CMakeLists.txt`
  - `contrib/CMakeLists.txt`
  - `src/configure_config.cmake`
  - `src/Common/config.h.in`
  - `src/CMakeLists.txt`
- MVP PDX implementation: `contrib/pdx/include/pdx/index.hpp` (minimal header-only prototype).
- Upstream reference for the initial baseline: `contrib/pdx/UPSTREAM_COMMIT`.
