#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/castColumn.h>
#include <Dictionaries/IDictionary.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>
#include <Common/TargetSpecific.h>
#include <Common/VectorQuantizer.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>

/// assignCentroid(vec, centroids) -> UInt32 cluster id: the index of the nearest (L2) centroid to vec.
///
/// The second argument is a CONSTANT and may be either:
///   * Array(Array(Float32)) - the centroids inline. The returned id is the position in this array
///     (matching the usual arrayJoin(hierarchicalKMeans(...)) + rowNumberInAllBlocks() convention).
///   * String - the name of a Dictionary holding columns (cid UInt*, vec Array(Float32)). The centroids
///     are read once and cached (per dictionary version); the returned id is the dictionary's cid.
///
/// Both forms share one kernel. The centroids are materialized into a column-major matrix ONCE per call
/// (from the const value, or from the cached dictionary read), then every row in the block is scored against
/// all centroids via the reformulation argmin_c ||x - c||^2 = argmin_c(||c||^2 - 2 x.c).
///
/// Past QUANTIZED_CENTROID_THRESHOLD centroids the exact scan is too slow to be useful and the answer
/// becomes APPROXIMATE: a `RaBitQIndex` shortlists RESCORE_CANDIDATES centroids by popcounts over 1-bit
/// codes and only those are scored exactly. See `assignApproximately`.

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Named (not anonymous) so the TargetSpecific::* namespaces the macro generates cannot collide with
/// identically named kernels from another translation unit.
namespace AssignCentroidImpl
{
namespace
{

/// The register-blocking shape of `scoreTile`. 6 x 16 accumulators fill 12 of the 16 AVX2 vector registers
/// and leave the rest for operands, so the accumulators stay in registers rather than in L1, where each
/// multiply-add would cost a load and a store.
constexpr size_t ROW_BLOCK = 6;
constexpr size_t COL_BLOCK = 16;

DECLARE_MULTITARGET_CODE(

/// Score every row against one tile of centroids, keeping the running best score and id per row.
/// See `build` and `assignBlock` for how the tile is prepared.
///
/// Rows are handled ROW_BLOCK at a time rather than one at a time: scoring a single row re-reads the whole
/// tile, which is bandwidth-bound at realistic sizes, while a block of rows reuses each tile value
/// ROW_BLOCK times. Measured 3.5x at 32768 centroids of dimension 768. The blocking shape follows
/// Goto & van de Geijn, "Anatomy of High-Performance Matrix Multiplication", ACM TOMS 34(3), 2008.
void scoreTile(
    const Float32 * __restrict vec_data, size_t num_rows, size_t dim,
    const Float32 * __restrict tile_centroids, const Float32 * __restrict tile_sq_norms, const UInt32 * __restrict tile_ids,
    size_t width, Float32 * __restrict best_score, UInt32 * __restrict result_ids)
{
    /// Score this row against `count` centroids and keep the best seen so far.
    auto reduce_row = [&](size_t row, const Float32 * __restrict dots, size_t block_start, size_t count)
    {
        Float32 best = best_score[row];
        UInt32 best_id = result_ids[row];
        for (size_t col = 0; col < count; ++col)
        {
            const Float32 score = tile_sq_norms[block_start + col] - 2.0f * dots[col];
            if (score < best)
            {
                best = score;
                best_id = tile_ids[block_start + col];
            }
        }
        best_score[row] = best;
        result_ids[row] = best_id;
    };

    size_t row = 0;
    for (; row + ROW_BLOCK <= num_rows; row += ROW_BLOCK) /// ROW_BLOCK incoming vectors at a time
    {
        for (size_t block_start = 0; block_start < width; block_start += COL_BLOCK) /// COL_BLOCK centroids at a time
        {
            Float32 dots[ROW_BLOCK][COL_BLOCK] = {};

            for (size_t coord = 0; coord < dim; ++coord) /// one per dimension
            {
                const Float32 * __restrict column = tile_centroids + coord * width + block_start;
                for (size_t block_row = 0; block_row < ROW_BLOCK; ++block_row)
                {
                    const Float32 coord_value = vec_data[(row + block_row) * dim + coord];
                    for (size_t col = 0; col < COL_BLOCK; ++col)
                        dots[block_row][col] += coord_value * column[col];
                }
            }

            for (size_t block_row = 0; block_row < ROW_BLOCK; ++block_row)
                reduce_row(row + block_row, dots[block_row], block_start, COL_BLOCK);
        }
    }

    /// Tail rows that do not fill a whole block.
    for (; row < num_rows; ++row)
    {
        for (size_t block_start = 0; block_start < width; block_start += COL_BLOCK)
        {
            Float32 dots[COL_BLOCK] = {};

            for (size_t coord = 0; coord < dim; ++coord)
            {
                const Float32 coord_value = vec_data[row * dim + coord];
                const Float32 * __restrict column = tile_centroids + coord * width + block_start;
                for (size_t col = 0; col < COL_BLOCK; ++col)
                    dots[col] += coord_value * column[col];
            }

            reduce_row(row, dots, block_start, COL_BLOCK);
        }
    }
}

/// Like `scoreTile`, but writes the whole score matrix instead of reducing to the best: the two-level form
/// needs the SUPER_PROBES nearest coarse centroids, and at a few hundred of them the matrix is small enough
/// to select from afterwards.
///
/// `scores` is row-major with `stride` entries per row; this fills columns `[tile_start, tile_start + width)`.
void scoreAllTile(
    const Float32 * __restrict vec_data, size_t num_rows, size_t dim,
    const Float32 * __restrict tile_centroids, const Float32 * __restrict tile_sq_norms,
    size_t width, size_t tile_start, size_t stride, Float32 * __restrict scores)
{
    size_t row = 0;
    for (; row + ROW_BLOCK <= num_rows; row += ROW_BLOCK)
    {
        for (size_t block_start = 0; block_start < width; block_start += COL_BLOCK)
        {
            Float32 dots[ROW_BLOCK][COL_BLOCK] = {};

            for (size_t coord = 0; coord < dim; ++coord)
            {
                const Float32 * __restrict column = tile_centroids + coord * width + block_start;
                for (size_t block_row = 0; block_row < ROW_BLOCK; ++block_row)
                {
                    const Float32 coord_value = vec_data[(row + block_row) * dim + coord];
                    for (size_t col = 0; col < COL_BLOCK; ++col)
                        dots[block_row][col] += coord_value * column[col];
                }
            }

            for (size_t block_row = 0; block_row < ROW_BLOCK; ++block_row)
                for (size_t col = 0; col < COL_BLOCK; ++col)
                    scores[(row + block_row) * stride + tile_start + block_start + col]
                        = tile_sq_norms[block_start + col] - 2.0f * dots[block_row][col];
        }
    }

    for (; row < num_rows; ++row)
    {
        for (size_t block_start = 0; block_start < width; block_start += COL_BLOCK)
        {
            Float32 dots[COL_BLOCK] = {};

            for (size_t coord = 0; coord < dim; ++coord)
            {
                const Float32 coord_value = vec_data[row * dim + coord];
                const Float32 * __restrict column = tile_centroids + coord * width + block_start;
                for (size_t col = 0; col < COL_BLOCK; ++col)
                    dots[col] += coord_value * column[col];
            }

            for (size_t col = 0; col < COL_BLOCK; ++col)
                scores[row * stride + tile_start + block_start + col]
                    = tile_sq_norms[block_start + col] - 2.0f * dots[col];
        }
    }
}

/// Score one vector against a shortlist of centroids given by position and return the id of the nearest.
///
/// No tiling or register blocking, unlike `scoreTile`: the shortlist is short and its centroids are
/// scattered, so each is read row-major and used once.
UInt32 rescoreCandidates(
    const Float32 * __restrict vec, size_t dim, const Float32 * __restrict centroids_row_major,
    const Float32 * __restrict centroid_sq_norms, const UInt32 * __restrict ids,
    const UInt32 * __restrict candidates, size_t num_candidates)
{
    /// Independent partial sums, not one running total. Float addition does not associate, so a single
    /// accumulator leaves the compiler no choice but one scalar FMA after another, each waiting on the
    /// previous one's latency - 4 cycles per coordinate. Choosing the summation order here rather than
    /// handing it to `-ffast-math` gets one FMA per ACCUMULATORS coordinates.
    static constexpr size_t ACCUMULATORS = 16;

    Float32 best = std::numeric_limits<Float32>::max();
    UInt32 best_id = ids[candidates[0]];
    for (size_t i = 0; i < num_candidates; ++i)
    {
        const UInt32 candidate = candidates[i];
        const Float32 * __restrict centroid = centroids_row_major + static_cast<size_t>(candidate) * dim;

        Float32 partial[ACCUMULATORS] = {};
        size_t coord = 0;
        for (; coord + ACCUMULATORS <= dim; coord += ACCUMULATORS)
            for (size_t lane = 0; lane < ACCUMULATORS; ++lane)
                partial[lane] += vec[coord + lane] * centroid[coord + lane];

        Float32 dot = 0;
        for (; coord < dim; ++coord)
            dot += vec[coord] * centroid[coord];
        for (Float32 partial_sum : partial)
            dot += partial_sum;

        const Float32 score = centroid_sq_norms[candidate] - 2.0f * dot;
        if (score < best)
        {
            best = score;
            best_id = ids[candidate];
        }
    }
    return best_id;
}

) // DECLARE_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is off (ARM, or
/// ENABLE_MULTITARGET_CODE=OFF) only Default exists, hence the plain loops above that auto-vectorize.
/// SIMD only, deliberately: executeImpl already runs on one pipeline thread per block, so threading here
/// would only oversubscribe.
void scoreTile(
    const Float32 * vec_data, size_t num_rows, size_t dim,
    const Float32 * tile_centroids, const Float32 * tile_sq_norms, const UInt32 * tile_ids,
    size_t width, Float32 * best_score, UInt32 * result_ids)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scoreTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, tile_ids, width, best_score, result_ids);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scoreTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, tile_ids, width, best_score, result_ids);
        return;
    }
#endif
    TargetSpecific::Default::scoreTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, tile_ids, width, best_score, result_ids);
}

void scoreAllTile(
    const Float32 * vec_data, size_t num_rows, size_t dim, const Float32 * tile_centroids,
    const Float32 * tile_sq_norms, size_t width, size_t tile_start, size_t stride, Float32 * scores)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scoreAllTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, tile_start, stride, scores);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scoreAllTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, tile_start, stride, scores);
        return;
    }
#endif
    TargetSpecific::Default::scoreAllTile(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, tile_start, stride, scores);
}

UInt32 rescoreCandidates(
    const Float32 * vec, size_t dim, const Float32 * centroids_row_major, const Float32 * centroid_sq_norms,
    const UInt32 * ids, const UInt32 * candidates, size_t num_candidates)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return TargetSpecific::x86_64_v4::rescoreCandidates(vec, dim, centroids_row_major, centroid_sq_norms, ids, candidates, num_candidates);
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::rescoreCandidates(vec, dim, centroids_row_major, centroid_sq_norms, ids, candidates, num_candidates);
#endif
    return TargetSpecific::Default::rescoreCandidates(vec, dim, centroids_row_major, centroid_sq_norms, ids, candidates, num_candidates);
}

}
}

namespace
{

/// The largest coordinate the Float32 scoring math can still handle. Squares are summed in Float32, so a
/// finite but huge coordinate overflows to infinity, the score becomes NaN, no comparison against the
/// running best is true, and the row silently takes the fallback id. `sqrt(FLT_MAX / (4 * dim))` keeps the
/// sum of squares and the dot product finite. At dim = 768 that is ~3.3e17, far above any real embedding.
Float32 coordinateLimit(size_t dim)
{
    return static_cast<Float32>(
        std::sqrt(static_cast<double>(std::numeric_limits<Float32>::max()) / (4.0 * static_cast<double>(dim))));
}

/// Past this many centroids `build` switches to the approximate path, and `assignCentroid` no longer always
/// returns the true nearest centroid. See `assignApproximately`.
constexpr size_t QUANTIZED_CENTROID_THRESHOLD = 32768;

/// How many centroids the approximate scan shortlists per row for exact scoring. One exact distance each,
/// a few per cent of the shortlisting itself at 768 dimensions.
constexpr size_t RESCORE_CANDIDATES = 100;

/// The centroids in the layout the scoring kernels read, plus squared norms and the id to return per centroid.
///
/// Which layout depends on the count. Up to QUANTIZED_CENTROID_THRESHOLD every row is scored against every
/// centroid, which wants column-major. Above it only a shortlist is scored, which wants row-major, because
/// a shortlist touches whole centroids rather than whole coordinates. Exactly one of the two is populated.
struct CentroidMatrix
{
    size_t num_centroids = 0;
    size_t dim = 0;

    /// Column-major: `centroids_transposed[coord * num_centroids + c]` is coordinate `coord` of centroid `c`.
    VectorWithMemoryTracking<Float32> centroids_transposed;

    /// Row-major: `centroids_row_major[c * dim + coord]`. Used instead of the above on the approximate path.
    VectorWithMemoryTracking<Float32> centroids_row_major;
    std::unique_ptr<RaBitQIndex> index;

    VectorWithMemoryTracking<Float32> centroid_sq_norms;   /// the squared norm of each centroid
    VectorWithMemoryTracking<UInt32> ids;                  /// the id to return when that centroid is nearest

    /// Pack the centroids into the layout the kernel reads. `id_values` gives the id per centroid, or null
    /// to use 0..num_centroids-1. Runs once per block for the inline form, and once per dictionary version
    /// for the dictionary form - never per row. `row_major` is consumed: the approximate path keeps it as
    /// its own copy instead of transposing it into one.
    ///
    /// For three centroids of dimension 2, `row_major` = [[1,2], [3,4], [5,6]]:
    ///
    ///     centroids_transposed = [1, 3, 5,  2, 4, 6]   coordinate 0 of every centroid, then coordinate 1
    ///     centroid_sq_norms    = [5, 25, 61]           1*1+2*2, 3*3+4*4, 5*5+6*6
    ///     ids                  = [0, 1, 2]             or the dictionary cids when id_values is given
    void build(VectorWithMemoryTracking<Float32> && row_major_, size_t num_centroids_, size_t dim_, const UInt32 * id_values)
    {
        num_centroids = num_centroids_;
        dim = dim_;
        const Float32 * row_major = row_major_.data();

        /// `rabitq` packs sign bits 8 to the byte, so a dimension that is not a multiple of 8 stays exact
        /// however many centroids there are.
        const bool approximate = num_centroids > QUANTIZED_CENTROID_THRESHOLD && RaBitQIndex::supportsDimensions(dim);
        if (!approximate)
            centroids_transposed.assign(dim * num_centroids, 0.0f);
        centroid_sq_norms.assign(num_centroids, 0.0f);
        ids.resize(num_centroids);
        const Float32 limit = coordinateLimit(dim);
        for (size_t centroid_index = 0; centroid_index < num_centroids; ++centroid_index)
        {
            const Float32 * centroid = row_major + centroid_index * dim;
            double sq_norm = 0;
            for (size_t coord = 0; coord < dim; ++coord)
            {
                /// A centroid the kernel cannot score is silently unreachable rather than an error, so both
                /// checks belong here. Free: this loop already reads every coordinate to build the norm.
                if (!std::isfinite(centroid[coord]))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} must not contain non-finite values (NaN or Inf)", centroid_index);
                if (std::abs(centroid[coord]) > limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} has coordinate {}, above the largest magnitude the "
                        "Float32 scoring math can represent for dimension {} ({})", centroid_index, centroid[coord], dim, limit);
                if (!approximate)
                    centroids_transposed[coord * num_centroids + centroid_index] = centroid[coord];
                sq_norm += static_cast<double>(centroid[coord]) * static_cast<double>(centroid[coord]);
            }
            centroid_sq_norms[centroid_index] = static_cast<Float32>(sq_norm);
            ids[centroid_index] = id_values ? id_values[centroid_index] : static_cast<UInt32>(centroid_index);
        }

        if (approximate)
        {
            index = std::make_unique<RaBitQIndex>(row_major, num_centroids, dim);
            centroids_row_major = std::move(row_major_);
        }
    }

    /// Assign every vector in a block to its nearest centroid id, writing into `result_ids` (already sized).
    ///
    /// The block arrives in ClickHouse's array layout. For three rows [[1,2], [3,4,5], [6]]:
    ///     vec_data = [1, 2, 3, 4, 5, 6]   every row's floats, concatenated
    ///     offsets  = [2, 5, 6]            where each row ends, exclusive
    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t num_rows, PaddedPODArray<UInt32> & result_ids) const
    {
        /// Both builders reject an empty or zero-dimension centroid set, so reaching here with either at
        /// zero is a bug, not bad input. `dim` divides the tile size below.
        if (num_centroids == 0 || dim == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "assignCentroid: centroid matrix is empty (num_centroids = {}, dim = {})", num_centroids, dim);

        for (size_t row = 0; row < num_rows; ++row) /// checked up front: the scoring loop assumes dense rows
        {
            size_t start = row ? offsets[row - 1] : 0;
            size_t length = offsets[row] - start;
            if (length != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: input vector has {} dimensions but centroids have {}", length, dim);
        }

        /// A NaN probe never beats the running best, so it would fall through to the `ids[0]` fallback and
        /// return a plausible-looking id instead of failing. The check above established that the rows are
        /// dense, so the payload is exactly `num_rows * dim` floats and can be swept linearly.
        const Float32 limit = coordinateLimit(dim);
        for (size_t i = 0; i < num_rows * dim; ++i)
        {
            if (!std::isfinite(vec_data[i]))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input vector must not contain non-finite values (NaN or Inf)");
            if (std::abs(vec_data[i]) > limit)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input coordinate {} is above the largest magnitude the Float32 scoring "
                    "math can represent for dimension {} ({})", vec_data[i], dim, limit);
        }

        if (index)
        {
            assignApproximately(vec_data, num_rows, result_ids);
            return;
        }

        VectorWithMemoryTracking<Float32> best_score(num_rows, std::numeric_limits<Float32>::max());
        for (size_t row = 0; row < num_rows; ++row)
            result_ids[row] = ids[0];

        /// Score against the centroids one tile at a time, where a tile is sized to stay in L2.
        ///
        /// At 32768 centroids of dimension 768 the whole matrix is 100 MB, and every input vector has to be
        /// compared against all of it. Sweeping all 100 MB once per vector reads from DRAM throughout.
        /// Taking a tile at a time and scoring every row against it keeps the centroids in cache instead.
        /// (This tile is about L2; ROW_BLOCK and COL_BLOCK in `scoreTile` are about registers.)
        ///
        /// For dimension 768: 512 KB / (768 * 4 B) = 170, rounded down to a multiple of COL_BLOCK -> 160.
        static constexpr size_t L2_TILE_BYTES = 512 * 1024;
        constexpr size_t col_block = AssignCentroidImpl::COL_BLOCK;
        const size_t tile = std::clamp<size_t>(
            (L2_TILE_BYTES / (dim * sizeof(Float32))) / col_block * col_block, col_block, 1024);

        VectorWithMemoryTracking<Float32> tile_centroids(tile * dim);
        VectorWithMemoryTracking<Float32> tile_sq_norms(tile);
        VectorWithMemoryTracking<UInt32> tile_ids(tile);

        /// Note the tile increment. This loop will run for 32768/160 = 205 times for the example.
        for (size_t tile_start = 0; tile_start < num_centroids; tile_start += tile)
        {
            const size_t width = std::min(tile, num_centroids - tile_start);
            const size_t padded = (width + col_block - 1) / col_block * col_block;

            for (size_t coord = 0; coord < dim; ++coord)
            {
                Float32 * tile_row = tile_centroids.data() + coord * padded;

                /// `build` left the centroids column-major, so one coordinate of every centroid in the
                /// tile is already contiguous.
                std::copy(&centroids_transposed[coord * num_centroids + tile_start], &centroids_transposed[coord * num_centroids + tile_start] + width, tile_row);

                std::fill(tile_row + width, tile_row + padded, 0.0f); /// if any padding
            }
            /// The squared norms and ids for the same tile.
            std::copy(&centroid_sq_norms[tile_start], &centroid_sq_norms[tile_start] + width, tile_sq_norms.begin());
            std::fill(tile_sq_norms.begin() + width, tile_sq_norms.begin() + padded, std::numeric_limits<Float32>::infinity());
            std::copy(&ids[tile_start], &ids[tile_start] + width, tile_ids.begin());
            std::fill(tile_ids.begin() + width, tile_ids.begin() + padded, 0u);

            /// Every row in the block is scored against this tile; `best_score` and `result_ids` carry the
            /// running winner across tiles.
            AssignCentroidImpl::scoreTile(
                vec_data, num_rows, dim, tile_centroids.data(), tile_sq_norms.data(), tile_ids.data(), padded,
                best_score.data(), result_ids.data());
        }
    }

    /// Shortlist RESCORE_CANDIDATES centroids per row from the 1-bit codes, then score the shortlist
    /// exactly. The answer is the nearest centroid whenever the true nearest reaches the shortlist, and one
    /// of the RESCORE_CANDIDATES nearest-looking centroids otherwise; how often depends on the data.
    ///
    /// Rows go in chunks so the candidate lists are still in cache when they are read back.
    void assignApproximately(const Float32 * vec_data, size_t num_rows, PaddedPODArray<UInt32> & result_ids) const
    {
        static constexpr size_t ROW_CHUNK = 1024;
        const size_t candidates_per_row = std::min(RESCORE_CANDIDATES, num_centroids);

        VectorWithMemoryTracking<UInt32> candidates(std::min(num_rows, ROW_CHUNK) * candidates_per_row);
        for (size_t chunk_start = 0; chunk_start < num_rows; chunk_start += ROW_CHUNK)
        {
            const size_t chunk = std::min(ROW_CHUNK, num_rows - chunk_start);
            index->nearestByL2(vec_data + chunk_start * dim, chunk, candidates_per_row, candidates.data());

            /// The shortlist comes back unordered. Sorting costs nothing next to the exact scoring and
            /// gets two things: the scattered reads below go forwards through the row-major array, and an
            /// exact tie resolves to the lowest id the way the exact path resolves it.
            for (size_t row = 0; row < chunk; ++row)
            {
                UInt32 * row_candidates = candidates.data() + row * candidates_per_row;
                std::sort(row_candidates, row_candidates + candidates_per_row);
            }

            for (size_t row = 0; row < chunk; ++row)
                result_ids[chunk_start + row] = AssignCentroidImpl::rescoreCandidates(
                    vec_data + (chunk_start + row) * dim, dim, centroids_row_major.data(), centroid_sq_norms.data(),
                    ids.data(), candidates.data() + row * candidates_per_row, candidates_per_row);
        }
    }
};

/// How many coarse centroids to look inside. A fine centroid belongs to exactly one coarse cluster, so the
/// nearest fine centroid can sit just across a cluster boundary from the nearest coarse one; opening
/// several clusters recovers it, at the cost of one cluster's worth of scanning each.
constexpr size_t SUPER_PROBES = 16;

/// One centroid as read from a dictionary.
struct DictionaryCentroid
{
    UInt64 cid = 0;
    UInt64 super_id = 0;   /// only read for the fine level of the hierarchical form
    VectorWithMemoryTracking<Float32> vec;
};

/// The two-level form: a small set of coarse centroids partitions a large set of fine ones, and a row only
/// looks at the fine centroids under the SUPER_PROBES coarse centroids nearest to it.
///
/// This is what decouples the cost from the number of fine centroids. The flat form scores every fine
/// centroid however cheap each one is; here a row scores every coarse centroid - a few hundred - and then
/// SUPER_PROBES/num_supers of the fine ones.
struct HierarchicalCentroids
{
    size_t dim = 0;

    /// The coarse level, scored exactly and in full: it is small, and picking the right clusters matters
    /// more than what picking them costs. Column-major, the layout `scoreAllTile` reads.
    size_t num_supers = 0;
    VectorWithMemoryTracking<Float32> supers_transposed;
    VectorWithMemoryTracking<Float32> super_sq_norms;

    /// The fine level, ordered by coarse cluster so a cluster's members are a contiguous range of positions
    /// and the scan can take a cluster as a range.
    size_t num_fine = 0;
    VectorWithMemoryTracking<Float32> fine_row_major;
    VectorWithMemoryTracking<Float32> fine_sq_norms;
    VectorWithMemoryTracking<UInt32> fine_ids;
    /// `[super_begin[s], super_end[s])` are the fine positions under coarse centroid `s`.
    VectorWithMemoryTracking<UInt32> super_begin;
    VectorWithMemoryTracking<UInt32> super_end;
    /// Absent when the dimension is not a multiple of 8, in which case the opened clusters are scored exactly.
    std::unique_ptr<RaBitQIndex> fine_index;

    void build(
        VectorWithMemoryTracking<DictionaryCentroid> coarse, const String & coarse_name,
        VectorWithMemoryTracking<DictionaryCentroid> fine, const String & fine_name)
    {
        num_supers = coarse.size();
        num_fine = fine.size();
        dim = coarse[0].vec.size();
        if (fine[0].vec.size() != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "assignCentroid: coarse dictionary {} has dimension {} but fine dictionary {} has {}",
                coarse_name, dim, fine_name, fine[0].vec.size());

        const Float32 limit = coordinateLimit(dim);
        auto checkFinite = [&](const DictionaryCentroid & centroid, const String & where)
        {
            for (size_t coord = 0; coord < dim; ++coord)
            {
                if (!std::isfinite(centroid.vec[coord]))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} of dictionary {} must not contain non-finite values (NaN or Inf)",
                        centroid.cid, where);
                if (std::abs(centroid.vec[coord]) > limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} of dictionary {} has coordinate {}, above the largest magnitude "
                        "the Float32 scoring math can represent for dimension {} ({})",
                        centroid.cid, where, centroid.vec[coord], dim, limit);
            }
        };

        /// `readCentroidDictionary` returns them sorted by cid, so position `s` is the `s`-th smallest
        /// coarse cid - the numbering `super_index` maps a `super_id` into.
        supers_transposed.assign(dim * num_supers, 0.0f);
        super_sq_norms.assign(num_supers, 0.0f);
        HashMap<UInt64, UInt32> super_index;
        for (size_t s = 0; s < num_supers; ++s)
        {
            checkFinite(coarse[s], coarse_name);
            double sq_norm = 0;
            for (size_t coord = 0; coord < dim; ++coord)
            {
                supers_transposed[coord * num_supers + s] = coarse[s].vec[coord];
                sq_norm += static_cast<double>(coarse[s].vec[coord]) * static_cast<double>(coarse[s].vec[coord]);
            }
            super_sq_norms[s] = static_cast<Float32>(sq_norm);
            if (!super_index.insert({coarse[s].cid, static_cast<UInt32>(s)}).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: coarse dictionary {} has duplicate cid {}", coarse_name, coarse[s].cid);
        }

        /// Order the fine centroids by (coarse cluster, cid). Sorting rather than bucketing keeps the
        /// order inside a cluster deterministic, so an exact tie resolves to the lowest cid.
        VectorWithMemoryTracking<std::pair<UInt32, UInt32>> order(num_fine); /// (coarse position, fine position as read)
        for (size_t f = 0; f < num_fine; ++f)
        {
            checkFinite(fine[f], fine_name);
            const auto * found = super_index.find(fine[f].super_id);
            if (!found)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: fine dictionary {} centroid {} has super_id {}, which is not a cid of the "
                    "coarse dictionary {}", fine_name, fine[f].cid, fine[f].super_id, coarse_name);
            order[f] = {found->getMapped(), static_cast<UInt32>(f)};
        }
        std::sort(order.begin(), order.end());

        fine_row_major.resize(num_fine * dim);
        fine_sq_norms.resize(num_fine);
        fine_ids.resize(num_fine);
        super_begin.assign(num_supers, 0);
        super_end.assign(num_supers, 0);
        for (size_t position = 0; position < num_fine; ++position)
        {
            const DictionaryCentroid & centroid = fine[order[position].second];
            std::copy(centroid.vec.begin(), centroid.vec.end(), &fine_row_major[position * dim]);
            double sq_norm = 0;
            for (size_t coord = 0; coord < dim; ++coord)
                sq_norm += static_cast<double>(centroid.vec[coord]) * static_cast<double>(centroid.vec[coord]);
            fine_sq_norms[position] = static_cast<Float32>(sq_norm);
            fine_ids[position] = static_cast<UInt32>(centroid.cid);
        }
        /// The order is sorted by coarse position, so each cluster's members are one contiguous run.
        for (size_t position = 0; position < num_fine; ++position)
        {
            const UInt32 super = order[position].first;
            if (position == 0 || order[position - 1].first != super)
                super_begin[super] = static_cast<UInt32>(position);
            super_end[super] = static_cast<UInt32>(position + 1);
        }

        if (RaBitQIndex::supportsDimensions(dim))
            fine_index = std::make_unique<RaBitQIndex>(fine_row_major.data(), num_fine, dim);
    }

    /// Score the coarse level in full, open the SUPER_PROBES nearest clusters, take the nearest fine
    /// centroid inside them.
    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t num_rows, PaddedPODArray<UInt32> & result_ids) const
    {
        for (size_t row = 0; row < num_rows; ++row) /// checked up front: the scoring loops assume dense rows
        {
            size_t start = row ? offsets[row - 1] : 0;
            size_t length = offsets[row] - start;
            if (length != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: input vector has {} dimensions but centroids have {}", length, dim);
        }

        const Float32 limit = coordinateLimit(dim);
        for (size_t i = 0; i < num_rows * dim; ++i)
        {
            if (!std::isfinite(vec_data[i]))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input vector must not contain non-finite values (NaN or Inf)");
            if (std::abs(vec_data[i]) > limit)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input coordinate {} is above the largest magnitude the Float32 scoring "
                    "math can represent for dimension {} ({})", vec_data[i], dim, limit);
        }

        /// Rows go in chunks so the coarse score matrix is still in cache when it is selected from.
        static constexpr size_t ROW_CHUNK = 256;
        const size_t probes = std::min(SUPER_PROBES, num_supers);

        VectorWithMemoryTracking<Float32> super_scores(std::min(num_rows, ROW_CHUNK) * num_supers);
        VectorWithMemoryTracking<UInt32> ranked_supers(num_supers);
        VectorWithMemoryTracking<RaBitQIndex::PositionRange> ranges(probes);
        VectorWithMemoryTracking<UInt32> candidates;   /// sized by `assignRow`, kept here to reuse the allocation

        static constexpr size_t L2_TILE_BYTES = 512 * 1024;
        constexpr size_t col_block = AssignCentroidImpl::COL_BLOCK;
        const size_t tile = std::clamp<size_t>(
            (L2_TILE_BYTES / (dim * sizeof(Float32))) / col_block * col_block, col_block, 1024);
        VectorWithMemoryTracking<Float32> tile_centroids(tile * dim);
        VectorWithMemoryTracking<Float32> tile_sq_norms(tile);

        for (size_t chunk_start = 0; chunk_start < num_rows; chunk_start += ROW_CHUNK)
        {
            const size_t chunk = std::min(ROW_CHUNK, num_rows - chunk_start);
            const Float32 * chunk_vecs = vec_data + chunk_start * dim;

            for (size_t tile_start = 0; tile_start < num_supers; tile_start += tile)
            {
                const size_t width = std::min(tile, num_supers - tile_start);
                const size_t padded = (width + col_block - 1) / col_block * col_block;
                for (size_t coord = 0; coord < dim; ++coord)
                {
                    Float32 * tile_row = tile_centroids.data() + coord * padded;
                    std::copy(&supers_transposed[coord * num_supers + tile_start],
                              &supers_transposed[coord * num_supers + tile_start] + width, tile_row);
                    std::fill(tile_row + width, tile_row + padded, 0.0f);
                }
                std::copy(&super_sq_norms[tile_start], &super_sq_norms[tile_start] + width, tile_sq_norms.begin());
                /// Padding columns get an infinite score, so the selection below never picks one.
                std::fill(tile_sq_norms.begin() + width, tile_sq_norms.begin() + padded, std::numeric_limits<Float32>::infinity());

                AssignCentroidImpl::scoreAllTile(
                    chunk_vecs, chunk, dim, tile_centroids.data(), tile_sq_norms.data(), padded, tile_start,
                    num_supers, super_scores.data());
            }

            for (size_t row = 0; row < chunk; ++row)
            {
                const Float32 * row_scores = super_scores.data() + row * num_supers;
                for (size_t s = 0; s < num_supers; ++s)
                    ranked_supers[s] = static_cast<UInt32>(s);
                std::nth_element(ranked_supers.begin(), ranked_supers.begin() + probes, ranked_supers.end(),
                    [&](UInt32 a, UInt32 b) { return row_scores[a] < row_scores[b]; });

                /// Sorted by position, so the fine scan walks the codes forwards.
                ranges.clear();
                for (size_t i = 0; i < probes; ++i)
                {
                    const UInt32 super = ranked_supers[i];
                    if (super_begin[super] < super_end[super])
                        ranges.push_back({super_begin[super], super_end[super]});
                }
                std::sort(ranges.begin(), ranges.end(),
                    [](const auto & a, const auto & b) { return a.begin < b.begin; });

                result_ids[chunk_start + row] = assignRow(chunk_vecs + row * dim, ranges, candidates);
            }
        }
    }

private:
    /// The nearest fine centroid inside the opened clusters. `candidates` is reused scratch.
    UInt32 assignRow(
        const Float32 * vec, const VectorWithMemoryTracking<RaBitQIndex::PositionRange> & ranges,
        VectorWithMemoryTracking<UInt32> & candidates) const
    {
        size_t num_candidates = 0;
        if (fine_index)
        {
            /// Shortlist from the 1-bit codes, then score the shortlist exactly.
            candidates.resize(RESCORE_CANDIDATES);
            num_candidates = fine_index->nearestByL2InRanges(
                vec, ranges.data(), ranges.size(), RESCORE_CANDIDATES, candidates.data());
            std::sort(candidates.begin(), candidates.begin() + num_candidates);
        }
        else
        {
            /// No codes for this dimension, so score every position in the opened clusters.
            candidates.clear();
            for (const auto & range : ranges)
                for (UInt32 position = range.begin; position < range.end; ++position)
                    candidates.push_back(position);
            num_candidates = candidates.size();
        }

        if (num_candidates == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "assignCentroid: the {} nearest coarse centroids hold no fine centroids at all", SUPER_PROBES);

        return AssignCentroidImpl::rescoreCandidates(
            vec, dim, fine_row_major.data(), fine_sq_norms.data(), fine_ids.data(), candidates.data(), num_candidates);
    }
};

/// A process-wide cache of the structures built from dictionaries.
///
/// Process-wide rather than a member because the function object is created afresh for every query: a member
/// cache rebuilds the structure each time, and at a few hundred thousand centroids that build takes seconds.
///
/// Entries are keyed on the dictionaries they were built from, held as `weak_ptr`. Expression actions can
/// outlive a query, and comparing raw addresses is unsafe across a reload - the old dictionary can be
/// destroyed and a later one allocated at the same address, which would hand back a structure built from the
/// previous version. A `weak_ptr` expires with the object it pointed at, so a reused address can never look
/// like a hit, and it does not keep the old dictionary alive.
///
/// Only CACHE_ENTRIES are kept: each holds its own copy of the centroids, hundreds of megabytes at the sizes
/// this path is for.
template <typename Built>
class DictionaryBuiltCache
{
public:
    using DictionaryPtr = std::shared_ptr<const IDictionary>;

    std::shared_ptr<const Built> get(const DictionaryPtr & first, const DictionaryPtr & second)
    {
        std::lock_guard lock(mutex);
        std::erase_if(entries, [](const Entry & entry) { return entry.first.expired(); });
        for (const Entry & entry : entries)
            if (entry.first.lock() == first && entry.second.lock() == second)
                return entry.built;
        return nullptr;
    }

    /// Building happens outside the lock, so two queries missing on a cold cache each build one and the
    /// second insert wins. That wastes a build; holding the lock across it would instead make every query
    /// wait on an unrelated dictionary's build.
    void put(const DictionaryPtr & first, const DictionaryPtr & second, std::shared_ptr<const Built> built)
    {
        std::lock_guard lock(mutex);
        std::erase_if(entries, [&](const Entry & entry)
            { return entry.first.expired() || (entry.first.lock() == first && entry.second.lock() == second); });
        if (entries.size() >= CACHE_ENTRIES)
            entries.erase(entries.begin());
        entries.push_back({first, second, std::move(built)});
    }

private:
    static constexpr size_t CACHE_ENTRIES = 2;

    struct Entry
    {
        std::weak_ptr<const IDictionary> first;
        std::weak_ptr<const IDictionary> second;  /// null for the one-dictionary form
        std::shared_ptr<const Built> built;
    };

    std::mutex mutex;
    VectorWithMemoryTracking<Entry> entries;
};

class FunctionAssignCentroid : public IFunction
{
public:
    static constexpr auto name = "assignCentroid";

    explicit FunctionAssignCentroid(ContextPtr context_) : dict_helper(std::move(context_)) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAssignCentroid>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }   /// 2 arguments for the flat form, 3 for two levels
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; } /// dictionary form depends on external, mutable state
    bool isSuitableForConstantFolding() const override { return false; }
    /// Only kicks in when every argument is constant, and `getArgumentsThatAreAlwaysConstant` keeps the
    /// centroids a `ColumnConst` even then, which is what the matrix builder expects.
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 or 3 arguments: assignCentroid(vec, centroids | dict_name) or "
                "assignCentroid(vec, coarse_dict_name, fine_dict_name)", name);

        const auto * vec_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!vec_type || !isFloat(vec_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be an array of floats", name);

        if (arguments.size() == 3)
        {
            if (!isString(arguments[1]) || !isString(arguments[2]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The two-level form of {} takes two constant Strings, the name of the coarse dictionary "
                    "and the name of the fine one; inline centroids are only supported by the two-argument form", name);
        }
        else if (!isCentroidsArray(arguments[1]) && !isString(arguments[1]))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a constant array of float arrays (the centroids) "
                "or a constant String (a dictionary name)", name);
        }

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// A distributed initiator can see zero rows with no local copy of the dictionary, so resolving one
        /// here would throw where an empty column is the answer.
        if (input_rows_count == 0)
            return ColumnUInt32::create();

        /// Shared, not owned: the dictionary forms hand back a cached structure, which another thread may
        /// swap, so the refcount is what keeps this one alive for the duration of the call.
        /// Constness is enforced by the framework, see `getArgumentsThatAreAlwaysConstant`.
        std::shared_ptr<const CentroidMatrix> matrix;
        std::shared_ptr<const HierarchicalCentroids> hierarchy;
        if (arguments.size() == 3)
        {
            hierarchy = getHierarchy(
                assert_cast<const ColumnConst &>(*arguments[1].column).getValue<String>(),
                assert_cast<const ColumnConst &>(*arguments[2].column).getValue<String>());
        }
        else if (isString(arguments[1].type))
        {
            matrix = getDictionaryMatrix(assert_cast<const ColumnConst &>(*arguments[1].column).getValue<String>());
        }
        else
        {
            matrix = buildConstMatrix(arguments[1]);
        }

        ColumnPtr vec_full = toFloat32Array(arguments[0]);
        const auto & vec = assert_cast<const ColumnArray &>(*vec_full);
        const auto & vec_data = assert_cast<const ColumnFloat32 &>(vec.getData()).getData();
        const auto & vec_offsets = vec.getOffsets();

        auto result = ColumnUInt32::create(input_rows_count);
        auto & res = result->getData();
        if (hierarchy)
            hierarchy->assignBlock(vec_data.data(), vec_offsets, input_rows_count, res);
        else
            matrix->assignBlock(vec_data.data(), vec_offsets, input_rows_count, res);
        return result;
    }

private:
    /// The kernels read Float32. Any other float width is converted once here rather than rejected, so
    /// `assignCentroid([1.0, 2.0], ...)` works with plain array literals, which are Array(Float64).
    static ColumnPtr toFloat32Array(const ColumnWithTypeAndName & arg)
    {
        static const DataTypePtr target = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
        ColumnWithTypeAndName full{arg.column->convertToFullColumnIfConst(), arg.type, arg.name};
        if (target->equals(*arg.type))
            return full.column;
        return castColumn(full, target);
    }

    mutable FunctionDictHelper dict_helper;

    static bool isCentroidsArray(const DataTypePtr & type)
    {
        const auto * outer = typeid_cast<const DataTypeArray *>(type.get());
        if (!outer)
            return false;
        const auto * inner = typeid_cast<const DataTypeArray *>(outer->getNestedType().get());
        return inner && isFloat(inner->getNestedType());
    }

    /// Build the matrix from a constant Array(Array(Float32)) argument. Ids are the array positions (0..k-1).
    static std::shared_ptr<const CentroidMatrix> buildConstMatrix(const ColumnWithTypeAndName & arg)
    {
        const auto & col_const = assert_cast<const ColumnConst &>(*arg.column);

        /// Convert to Array(Array(Float32)) first, so a Float64 or BFloat16 literal is accepted. The data
        /// column behind the constant carries `arg.type` itself, not its nested type.
        static const DataTypePtr target
            = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
        ColumnPtr casted = col_const.getDataColumnPtr();
        if (!target->equals(*arg.type))
            casted = castColumn({casted, arg.type, arg.name}, target);

        const auto & outer_array = assert_cast<const ColumnArray &>(*casted);                    /// one row = the num_centroids centroids
        const auto & inner_array = assert_cast<const ColumnArray &>(outer_array.getData());            /// num_centroids inner_array arrays
        const auto & values = assert_cast<const ColumnFloat32 &>(inner_array.getData()).getData();

        size_t num_centroids = outer_array.getOffsets()[0];
        if (num_centroids == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: centroids array is empty");

        size_t dim = inner_array.getOffsets()[0];
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: centroids have zero dimension");

        VectorWithMemoryTracking<Float32> row_major(num_centroids * dim);
        for (size_t centroid_index = 0; centroid_index < num_centroids; ++centroid_index)
        {
            size_t start = centroid_index ? inner_array.getOffsets()[centroid_index - 1] : 0;
            size_t length = inner_array.getOffsets()[centroid_index] - start;
            if (length != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: centroid {} has {} dimensions, expected {}", centroid_index, length, dim);
            std::copy(&values[start], &values[start + length], &row_major[centroid_index * dim]);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(std::move(row_major), num_centroids, dim, /*id_values=*/nullptr);
        return matrix;
    }

    /// Full-read a centroid dictionary, ordered by `cid`, the same way the `dictionary()` table function
    /// does. `with_super_id` also reads the `super_id` attribute, which the fine level uses to name the
    /// coarse centroid a fine centroid belongs to.
    static VectorWithMemoryTracking<DictionaryCentroid> readCentroidDictionary(
        const std::shared_ptr<const IDictionary> & dictionary, const String & dict_name, bool with_super_id)
    {
        Names columns{"cid", "vec"};
        if (with_super_id)
            columns.emplace_back("super_id");

        QueryPipeline pipeline(dictionary->read(columns, /*max_block_size=*/65536, /*num_streams=*/1));
        PullingPipelineExecutor executor(pipeline);

        auto checkUnsigned = [&](const ColumnWithTypeAndName & column, const char * attribute)
        {
            /// `getUInt` accepts any arithmetic column, so a `Float64` or `Int64` key would be silently cast
            /// and we would hand back an id the dictionary never stored. Check the type, not just the range.
            if (!WhichDataType(column.type).isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "assignCentroid: attribute `{}` of dictionary {} must be an unsigned integer, got {}",
                    attribute, dict_name, column.type->getName());
        };

        VectorWithMemoryTracking<DictionaryCentroid> centroids;
        Block block;
        while (executor.pull(block))
        {
            const auto & cid_with_type = block.getByName("cid");
            checkUnsigned(cid_with_type, "cid");
            const auto & cid_col = cid_with_type.column;

            ColumnPtr super_col;
            if (with_super_id)
            {
                const auto & super_with_type = block.getByName("super_id");
                checkUnsigned(super_with_type, "super_id");
                super_col = super_with_type.column;
            }

            const auto & vec_col = block.getByName("vec");

            /// The dictionary type is only known here (the name is a runtime string), and the kernels read
            /// the nested column as ColumnFloat32, so reject anything else instead of reinterpreting the payload.
            const auto * vec_type = typeid_cast<const DataTypeArray *>(vec_col.type.get());
            if (!vec_type || !WhichDataType(vec_type->getNestedType()).isFloat32())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "assignCentroid: attribute `vec` of dictionary {} must be Array(Float32), got {}",
                    dict_name, vec_col.type->getName());

            const auto & vec_arr = assert_cast<const ColumnArray &>(*vec_col.column);
            const auto & vec_vals = assert_cast<const ColumnFloat32 &>(vec_arr.getData()).getData();
            const auto & vec_off = vec_arr.getOffsets();
            for (size_t row = 0; row < cid_col->size(); ++row)
            {
                size_t start = row ? vec_off[row - 1] : 0;
                size_t length = vec_off[row] - start;
                DictionaryCentroid centroid;
                centroid.cid = cid_col->getUInt(row);
                centroid.super_id = super_col ? super_col->getUInt(row) : 0;
                centroid.vec.assign(&vec_vals[start], &vec_vals[start + length]);
                centroids.push_back(std::move(centroid));
            }
        }

        if (centroids.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} produced no centroids", dict_name);

        std::sort(centroids.begin(), centroids.end(), [](const auto & a, const auto & b) { return a.cid < b.cid; });

        const size_t dim = centroids[0].vec.size();
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} has zero-dimension centroids", dict_name);
        for (const auto & centroid : centroids)
        {
            if (centroid.vec.size() != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: dictionary {} centroid {} has {} dimensions, expected {}",
                    dict_name, centroid.cid, centroid.vec.size(), dim);
            /// The result type is exactly UInt32 - reject anything greater.
            if (centroid.cid > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: dictionary {} has cid {} which exceeds the UInt32 range of the result",
                    dict_name, centroid.cid);
        }
        return centroids;
    }

    /// Read the named dictionary once (attributes cid, vec), cached until the dictionary reloads.
    std::shared_ptr<const CentroidMatrix> getDictionaryMatrix(const String & dict_name) const
    {
        auto dictionary = dict_helper.getDictionary(dict_name);

        static DictionaryBuiltCache<CentroidMatrix> cache;
        if (auto cached = cache.get(dictionary, nullptr))
            return cached;

        const auto centroids = readCentroidDictionary(dictionary, dict_name, /*with_super_id=*/false);
        const size_t num_centroids = centroids.size();
        const size_t dim = centroids[0].vec.size();

        VectorWithMemoryTracking<Float32> row_major(num_centroids * dim);
        VectorWithMemoryTracking<UInt32> ids(num_centroids);
        for (size_t centroid_index = 0; centroid_index < num_centroids; ++centroid_index)
        {
            std::copy(centroids[centroid_index].vec.begin(), centroids[centroid_index].vec.end(), &row_major[centroid_index * dim]);
            ids[centroid_index] = static_cast<UInt32>(centroids[centroid_index].cid);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(std::move(row_major), num_centroids, dim, ids.data());

        cache.put(dictionary, nullptr, matrix);
        return matrix;
    }

    /// Read both dictionaries once and build the two-level structure, cached until either reloads.
    std::shared_ptr<const HierarchicalCentroids> getHierarchy(const String & coarse_name, const String & fine_name) const
    {
        auto coarse_dictionary = dict_helper.getDictionary(coarse_name);
        auto fine_dictionary = dict_helper.getDictionary(fine_name);

        static DictionaryBuiltCache<HierarchicalCentroids> cache;
        if (auto cached = cache.get(coarse_dictionary, fine_dictionary))
            return cached;

        auto hierarchy = std::make_shared<HierarchicalCentroids>();
        hierarchy->build(
            readCentroidDictionary(coarse_dictionary, coarse_name, /*with_super_id=*/false), coarse_name,
            readCentroidDictionary(fine_dictionary, fine_name, /*with_super_id=*/true), fine_name);

        cache.put(coarse_dictionary, fine_dictionary, hierarchy);
        return hierarchy;
    }
};

}

REGISTER_FUNCTION(AssignCentroid)
{
    FunctionDocumentation::Description description =
        "Returns the id of the nearest (L2) centroid to a vector. The centroids are given as a constant "
        "array of float arrays, where the id is the 0-based position in that array, or as the name of a "
        "`Dictionary` holding the attributes `cid` and `vec`, where the id is `cid`.\n\n"
        "With more than 32768 centroids of a dimension that is a multiple of 8, the result is APPROXIMATE: "
        "the centroids are quantized to one bit per coordinate (RaBitQ), the 100 that look nearest under "
        "that quantization are scored exactly, and the best of those is returned. This is around an order of "
        "magnitude faster than comparing against every centroid, at the price of occasionally returning a "
        "centroid that is near rather than nearest.\n\n"
        "Given two dictionary names instead of one, the centroids are treated as two levels: the first "
        "dictionary holds a small set of coarse centroids, the second the fine centroids, each of which "
        "names its coarse centroid in a `super_id` attribute. A vector is then scored against every coarse "
        "centroid, and only against the fine centroids under the 16 nearest of them. This is the form to "
        "use for a large centroid set: its cost grows with the number of coarse centroids rather than with "
        "the number of fine ones.";
    FunctionDocumentation::Syntax syntax =
        "assignCentroid(vec, centroids | dict_name)\nassignCentroid(vec, coarse_dict_name, fine_dict_name)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Vector to assign. Its dimension must match the dimension of the centroids. Widths other than "
                "`Float32` are converted to `Float32`, which is what the scoring kernel uses.",
         {"Array(Float32)", "Array(Float64)", "Array(BFloat16)"}},
        {"centroids", "The centroids to score against, which must be constant. Given as an array of equally sized, "
                      "non-empty float arrays, the id then being the 0-based position in that array; or as the name of "
                      "a `Dictionary` with an attribute `cid` of an unsigned integer type that fits `UInt32` and an "
                      "attribute `vec` of type `Array(Float32)`, the id then being `cid`. The dictionary is read once "
                      "and cached until it reloads.",
         {"Array(Array(Float32))", "Array(Array(Float64))", "Array(Array(BFloat16))", "String"}},
        {"fine_dict_name", "Optional. The name of a `Dictionary` of fine centroids, with the attributes `cid` and "
                           "`vec` as above plus `super_id` of an unsigned integer type, naming the `cid` of the "
                           "coarse centroid the fine centroid belongs to. When it is given, the second argument is "
                           "the coarse dictionary and the returned id is a fine `cid`.",
         {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The nearest centroid id.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Inline centroids",
         "SELECT assignCentroid([1.0, 2.0]::Array(Float32), [[0.0, 0.0], [1.0, 2.0]]::Array(Array(Float32)))", "1"},
        {"Two levels",
         "SELECT assignCentroid(vec, 'coarse_centroids_dict', 'fine_centroids_dict') FROM embeddings", ""}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAssignCentroid>(documentation);
}

}
