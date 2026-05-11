#pragma once
#include "config.h"
#if USE_DISKANN

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace DB
{

enum class DiskANNMetric : uint8_t
{
    L2 = 0,
    Cosine = 1,
};

struct DiskANNBuildOptions
{
    uint32_t pruned_degree = 32;
    uint32_t max_degree = 64;
    uint32_t l_build = 128;
    float alpha = 1.2f;
    uint32_t num_threads = 1;
    uint32_t pq_chunks = 4;
    double build_ram_limit_gb = 0.25;
};

struct DiskANNSearchOptions
{
    uint32_t num_threads = 1;
    uint32_t search_io_limit = 4;
    uint32_t num_nodes_to_cache = 0;
    uint32_t default_search_list_size = 64;
    uint32_t default_beam_width = 4;
};

class DiskANNDiskIndexBuilder
{
public:
    DiskANNDiskIndexBuilder(
        size_t dimensions,
        DiskANNMetric metric,
        DiskANNBuildOptions options = {});

    ~DiskANNDiskIndexBuilder();

    DiskANNDiskIndexBuilder(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder & operator=(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder(DiskANNDiskIndexBuilder && other) noexcept;
    DiskANNDiskIndexBuilder & operator=(DiskANNDiskIndexBuilder && other) noexcept;

    void setDataPath(const std::string & path);
    void setIndexPrefix(const std::string & prefix);
    void build() const;

    static bool indexFilesExist(const std::string & index_prefix);

private:
    int64_t handle = -1;
    size_t dim;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

class DiskANNDiskIndexSearcher
{
public:
    DiskANNDiskIndexSearcher(
        size_t dimensions,
        DiskANNMetric metric,
        const std::string & index_prefix,
        DiskANNSearchOptions options = {});

    ~DiskANNDiskIndexSearcher();

    DiskANNDiskIndexSearcher(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher & operator=(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher(DiskANNDiskIndexSearcher && other) noexcept;
    DiskANNDiskIndexSearcher & operator=(DiskANNDiskIndexSearcher && other) noexcept;

    size_t search(
        const float * query,
        size_t query_dim,
        size_t k,
        uint64_t * ids,
        float * distances,
        size_t search_list_size = 0,
        size_t beam_width = 0) const;

private:
    int64_t handle = -1;
    size_t dim;
    DiskANNSearchOptions options;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

using DiskANNDiskIndexBuilderPtr = std::shared_ptr<DiskANNDiskIndexBuilder>;
using DiskANNDiskIndexSearcherPtr = std::shared_ptr<DiskANNDiskIndexSearcher>;

/// Stateless batched distance kernel matching the index `metric`. Computes
///   `out[i] = distance(query, candidates + i * dim)` for `i` in `[0, n)`.
///
/// Uses the same SIMD kernel that DiskANN uses internally during graph search, so the
/// values are numerically consistent with the per-row distances DiskANN returns from
/// `search`. Intended for callers that want kernel parity with the index path without
/// holding a builder/searcher handle.
///
/// Result semantics (matches the FFI):
///   * `L2`     — squared L2 distance.
///   * `Cosine` — `1 - cosine_similarity`, valid for un-normalised vectors.
///
/// `query` must point to `dim` floats; `candidates` to `n * dim` floats laid out row-major;
/// `out` to `n` floats. All pointers must be non-null when `n > 0`. Throws `INCORRECT_DATA`
/// on FFI failure.
void DiskANNComputeDistances(
    DiskANNMetric metric,
    size_t dim,
    const float * query,
    const float * candidates,
    size_t n,
    float * out);

}

/// File-name conventions for artefacts produced by the DiskANN FFI layer. Kept in the same
/// header as the FFI wrappers so that a future change on the Rust side produces a
/// single-file diff in C++ as well.
///
/// These values mirror the hard-coded suffixes used by Rust
/// `diskann_providers::storage::get_*_file` (snapshot: 2026-04). If those names ever change,
/// update both sides in lock-step.
namespace DB::DiskANNArtifactNames
{
    /// Basename used as the `index_prefix` passed to the FFI builder. Stored in the group
    /// directory so that the full prefix is `<group_full_path>/idx`.
    constexpr const char * INDEX_PREFIX_BASENAME = "idx";

    /// Suffixes appended by DiskANN to the prefix to form the three required artefacts.
    constexpr const char * DISK_INDEX_SUFFIX = "_disk.index";
    constexpr const char * PQ_PIVOTS_SUFFIX = "_pq_pivots.bin";
    constexpr const char * PQ_COMPRESSED_SUFFIX = "_pq_compressed.bin";
}

#endif
