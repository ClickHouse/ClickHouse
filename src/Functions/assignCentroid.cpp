#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
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
#include <Common/assert_cast.h>
#include <Common/TargetSpecific.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <variant>

/// assignCentroid(vec, centroids) -> UInt32 cluster id: the index of the nearest (L2) centroid to vec.
///
/// The second argument is a CONSTANT and may be either:
///   * Array(Array(Float32)) - the centroids inline. The returned id is the position in this array
///     (matching the usual arrayJoin(hierarchicalKMeans(...)) + rowNumberInAllBlocks() convention).
///   * String - the name of a Dictionary holding columns (cid UInt*, vec Array(Float32)). The centroids
///     are read once and cached (per dictionary version); the returned id is the dictionary's cid.
///     With columns `level` and `parent` as well, the search is two-level, see `HierarchicalCentroids`.
///
/// Both forms share one kernel. The centroids are materialized into column-major tiles ONCE per call
/// (from the const value, or from the cached dictionary read), then every row in the block is scored against
/// all centroids via the reformulation argmin_c ||x - c||^2 = argmin_c(||c||^2 - 2 x.c).

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

DECLARE_X86_64_MULTITARGET_CODE(

/// Compute the dot products of every row with one tile of centroids, COL_BLOCK centroids at a time, and pass
/// each (row, block) to `on_block(row, dots, block_start)`. See `CentroidMatrix::build` for the tile layout.
///
/// Rows are handled ROW_BLOCK at a time rather than one at a time: scoring a single row re-reads the whole
/// tile, which is bandwidth-bound at realistic sizes, while a block of rows reuses each tile value
/// ROW_BLOCK times. Measured 3.5x at 32768 centroids of dimension 768. The blocking shape follows
/// Goto & van de Geijn, "Anatomy of High-Performance Matrix Multiplication", ACM TOMS 34(3), 2008.
template <typename OnBlock>
void scoreTileBlocks(
    const Float32 * __restrict vec_data, size_t num_rows, size_t dim,
    const Float32 * __restrict tile_centroids, size_t width, OnBlock && on_block)
{
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
                on_block(row + block_row, dots[block_row], block_start);
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

            on_block(row, dots, block_start);
        }
    }
}

/// Score every row against one tile of centroids, keeping the running best score and id per row.
void scoreTile(
    const Float32 * __restrict vec_data, size_t num_rows, size_t dim,
    const Float32 * __restrict tile_centroids, const Float32 * __restrict tile_sq_norms, const UInt32 * __restrict tile_ids,
    size_t width, Float32 * __restrict best_score, UInt32 * __restrict result_ids)
{
    scoreTileBlocks(vec_data, num_rows, dim, tile_centroids, width,
        [&](size_t row, const Float32 * __restrict dots, size_t block_start)
        {
            Float32 best = best_score[row];
            UInt32 best_id = result_ids[row];
            for (size_t col = 0; col < COL_BLOCK; ++col)
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
        });
}

/// Score every row against one tile of centroids, storing every score: row `r`, centroid `c` of the tile goes
/// to `scores[r * stride + c]`.
void scoreTileAll(
    const Float32 * __restrict vec_data, size_t num_rows, size_t dim,
    const Float32 * __restrict tile_centroids, const Float32 * __restrict tile_sq_norms,
    size_t width, Float32 * __restrict scores, size_t stride)
{
    scoreTileBlocks(vec_data, num_rows, dim, tile_centroids, width,
        [&](size_t row, const Float32 * __restrict dots, size_t block_start)
        {
            Float32 * __restrict row_scores = scores + row * stride + block_start;
            for (size_t col = 0; col < COL_BLOCK; ++col)
                row_scores[col] = tile_sq_norms[block_start + col] - 2.0f * dots[col];
        });
}

) // DECLARE_X86_64_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is off (ARM, or
/// ENABLE_MULTITARGET_CODE=OFF) only Default exists, hence the plain loops above that auto-vectorize.
/// SIMD only, deliberately: executeImpl already runs on one pipeline thread per block, so threading here
/// would only oversubscribe.
void scoreTile(
    const Float32 * vec_data, size_t num_rows, size_t dim,
    const Float32 * tile_centroids, const Float32 * tile_sq_norms, const UInt32 * tile_ids,
    size_t width, Float32 * best_score, UInt32 * result_ids)
{
#if USE_X86_64_MULTITARGET_CODE
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

void scoreTileAll(
    const Float32 * vec_data, size_t num_rows, size_t dim,
    const Float32 * tile_centroids, const Float32 * tile_sq_norms,
    size_t width, Float32 * scores, size_t stride)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scoreTileAll(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, scores, stride);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scoreTileAll(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, scores, stride);
        return;
    }
#endif
    TargetSpecific::Default::scoreTileAll(vec_data, num_rows, dim, tile_centroids, tile_sq_norms, width, scores, stride);
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

/// `n` rounded up to a whole number of kernel column blocks.
size_t roundUpToColBlock(size_t n)
{
    constexpr size_t col_block = AssignCentroidImpl::COL_BLOCK;
    return (n + col_block - 1) / col_block * col_block;
}

/// Centroids + squared norms + the id to return per centroid, stored in the tiles the kernel reads.
struct CentroidMatrix
{
    size_t num_centroids = 0;
    size_t dim = 0;
    size_t tile_width = 0;

    /// Tile `t` holds centroids [t * tile_width, min(num_centroids, (t + 1) * tile_width)) column-major from
    /// `tiles[t * tile_width * dim]`, its width rounded up to a whole COL_BLOCK with zero centroids.
    VectorWithMemoryTracking<Float32> tiles;

    VectorWithMemoryTracking<Float32> centroid_sq_norms;   /// the squared norm of each centroid, +inf for padding
    VectorWithMemoryTracking<UInt32> ids;                  /// the id to return when that centroid is nearest

    /// The length of `centroid_sq_norms` and `ids`.
    [[nodiscard]] size_t paddedSize() const { return roundUpToColBlock(num_centroids); }

    [[nodiscard]] size_t tilePaddedWidth(size_t tile_start) const { return roundUpToColBlock(std::min(tile_width, num_centroids - tile_start)); }

    /// Pack the centroids into the layout the kernel reads. `id_values` gives the id per centroid, or null
    /// to use 0..num_centroids-1. Runs once per block for the inline form, and once per dictionary version
    /// for the dictionary form - never per row.
    ///
    /// For three centroids of dimension 2, `row_major` = [[1,2], [3,4], [5,6]], one tile padded to 16 columns:
    ///
    ///     tiles             = [1, 3, 5, 0 x 13,  2, 4, 6, 0 x 13]   coordinate 0 of every centroid, then coordinate 1
    ///     centroid_sq_norms = [5, 25, 61, inf x 13]                 1*1+2*2, 3*3+4*4, 5*5+6*6
    ///     ids               = [0, 1, 2, 0 x 13]                     or the dictionary cids when id_values is given
    void build(const Float32 * row_major, size_t num_centroids_, size_t dim_, const UInt32 * id_values)
    {
        num_centroids = num_centroids_;
        dim = dim_;
        if (num_centroids == 0 || dim == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "assignCentroid: centroid matrix is empty (num_centroids = {}, dim = {})", num_centroids, dim);

        /// A tile is sized to stay in L2. At 32768 centroids of dimension 768 the whole matrix is 100 MB, and every
        /// input vector has to be compared against all of it. Sweeping all 100 MB once per vector reads from DRAM
        /// throughout. Taking a tile at a time and scoring every row against it keeps the centroids in cache instead.
        /// (This tile is about L2; ROW_BLOCK and COL_BLOCK in `scoreTile` are about registers.)
        ///
        /// For dimension 768: 512 KB / (768 * 4 B) = 170, rounded down to a multiple of COL_BLOCK -> 160.
        static constexpr size_t L2_TILE_BYTES = 512 * 1024;
        constexpr size_t col_block = AssignCentroidImpl::COL_BLOCK;
        tile_width = std::clamp<size_t>((L2_TILE_BYTES / (dim * sizeof(Float32))) / col_block * col_block, col_block, 1024);

        tiles.assign(paddedSize() * dim, 0.0f);
        centroid_sq_norms.assign(paddedSize(), std::numeric_limits<Float32>::infinity());
        ids.assign(paddedSize(), 0);
        const Float32 limit = coordinateLimit(dim);
        for (size_t centroid_index = 0; centroid_index < num_centroids; ++centroid_index)
        {
            const size_t tile_start = centroid_index / tile_width * tile_width;
            const size_t padded = tilePaddedWidth(tile_start);
            Float32 * tile = tiles.data() + tile_start * dim;
            const size_t column = centroid_index - tile_start;

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
                tile[coord * padded + column] = centroid[coord];
                sq_norm += static_cast<double>(centroid[coord]) * static_cast<double>(centroid[coord]);
            }
            centroid_sq_norms[centroid_index] = static_cast<Float32>(sq_norm);
            ids[centroid_index] = id_values ? id_values[centroid_index] : static_cast<UInt32>(centroid_index);
        }
    }

    /// Validate a block in ClickHouse's array layout. For three rows [[1,2], [3,4,5], [6]]:
    ///     vec_data = [1, 2, 3, 4, 5, 6]   every row's floats, concatenated
    ///     offsets  = [2, 5, 6]            where each row ends, exclusive
    static void checkInput(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t num_rows, size_t dim)
    {
        for (size_t row = 0; row < num_rows; ++row) /// checked up front: the scoring loop assumes dense rows
        {
            size_t start = row ? offsets[static_cast<ssize_t>(row) - 1] : 0;
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
    }

    /// Score `num_rows` dense rows against every centroid. `best_score` and `result_ids` carry the running
    /// winner per row, across tiles and across calls.
    void scoreRows(const Float32 * vec_data, size_t num_rows, Float32 * best_score, UInt32 * result_ids) const
    {
        for (size_t tile_start = 0; tile_start < num_centroids; tile_start += tile_width)
            AssignCentroidImpl::scoreTile(
                vec_data, num_rows, dim, tiles.data() + tile_start * dim, centroid_sq_norms.data() + tile_start,
                ids.data() + tile_start, tilePaddedWidth(tile_start), best_score, result_ids);
    }

    /// Store the score of row `r` against centroid `c` in `scores[r * stride + c]`, for every `c` < `paddedSize()`.
    void scoreAll(const Float32 * vec_data, size_t num_rows, Float32 * scores, size_t stride) const
    {
        for (size_t tile_start = 0; tile_start < num_centroids; tile_start += tile_width)
            AssignCentroidImpl::scoreTileAll(
                vec_data, num_rows, dim, tiles.data() + tile_start * dim, centroid_sq_norms.data() + tile_start,
                tilePaddedWidth(tile_start), scores + tile_start, stride);
    }

    /// Assign every vector in a block to its nearest centroid id, writing into `result_ids` (already sized).
    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t num_rows, PaddedPODArray<UInt32> & result_ids) const
    {
        /// Both builders reject an empty or zero-dimension centroid set, so reaching here with either at
        /// zero is a bug, not bad input.
        if (num_centroids == 0 || dim == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "assignCentroid: centroid matrix is empty (num_centroids = {}, dim = {})", num_centroids, dim);

        checkInput(vec_data, offsets, num_rows, dim);

        VectorWithMemoryTracking<Float32> best_score(num_rows, std::numeric_limits<Float32>::max());
        for (size_t row = 0; row < num_rows; ++row)
            result_ids[row] = ids[0];

        scoreRows(vec_data, num_rows, best_score.data(), result_ids.data());
    }
};

/// Leaf centroids grouped under coarse centroids. A row is scored against every coarse centroid, then against the
/// leaves of the COARSE_PROBES nearest ones only, so the result is the nearest leaf of those clusters.
struct HierarchicalCentroids
{
    static constexpr size_t COARSE_PROBES = 16;
    /// Bounds the coarse scores kept for one chunk of rows.
    static constexpr size_t SCORE_BUFFER_BYTES = 8 * 1024 * 1024;

    size_t dim = 0;
    CentroidMatrix coarse;                               /// ids are positions in `clusters`
    VectorWithMemoryTracking<CentroidMatrix> clusters;   /// the leaves of each coarse centroid, ids are leaf cids

    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t num_rows, PaddedPODArray<UInt32> & result_ids) const
    {
        CentroidMatrix::checkInput(vec_data, offsets, num_rows, dim);

        VectorWithMemoryTracking<Float32> best_score(num_rows, std::numeric_limits<Float32>::max());
        for (size_t row = 0; row < num_rows; ++row)
            result_ids[row] = clusters[0].ids[0];

        const size_t num_clusters = clusters.size();
        if (num_clusters <= COARSE_PROBES)
        {
            for (const auto & cluster : clusters)
                cluster.scoreRows(vec_data, num_rows, best_score.data(), result_ids.data());
            return;
        }

        const size_t stride = coarse.paddedSize();
        const size_t chunk_rows = std::clamp<size_t>(SCORE_BUFFER_BYTES / (stride * sizeof(Float32)), 1, num_rows);

        VectorWithMemoryTracking<Float32> scores(chunk_rows * stride);
        VectorWithMemoryTracking<UInt32> order(num_clusters);
        VectorWithMemoryTracking<UInt32> probes(chunk_rows * COARSE_PROBES);
        VectorWithMemoryTracking<size_t> cluster_begin(num_clusters + 1);
        VectorWithMemoryTracking<size_t> cluster_cursor(num_clusters);
        VectorWithMemoryTracking<UInt32> cluster_rows(chunk_rows * COARSE_PROBES);
        VectorWithMemoryTracking<Float32> gathered_vecs(chunk_rows * dim);
        VectorWithMemoryTracking<Float32> gathered_best(chunk_rows);
        VectorWithMemoryTracking<UInt32> gathered_ids(chunk_rows);

        for (size_t chunk_start = 0; chunk_start < num_rows; chunk_start += chunk_rows)
        {
            const size_t rows = std::min(chunk_rows, num_rows - chunk_start);
            coarse.scoreAll(vec_data + chunk_start * dim, rows, scores.data(), stride);

            /// The probes of a row are the COARSE_PROBES smallest under (score, position), so ties go to the lower coarse cid.
            std::fill(cluster_begin.begin(), cluster_begin.end(), 0);
            for (size_t row = 0; row < rows; ++row)
            {
                const Float32 * row_scores = scores.data() + row * stride;
                std::iota(order.begin(), order.end(), 0);
                std::nth_element(order.begin(), order.begin() + COARSE_PROBES, order.end(),
                    [row_scores](UInt32 a, UInt32 b) { return row_scores[a] < row_scores[b] || (row_scores[a] == row_scores[b] && a < b); });
                std::copy_n(order.begin(), COARSE_PROBES, probes.begin() + row * COARSE_PROBES);
                for (size_t probe = 0; probe < COARSE_PROBES; ++probe)
                    ++cluster_begin[order[probe] + 1];
            }

            /// Group the rows by probed cluster (a counting sort), so each cluster is scored once per chunk.
            for (size_t cluster = 0; cluster < num_clusters; ++cluster)
                cluster_begin[cluster + 1] += cluster_begin[cluster];
            std::copy_n(cluster_begin.begin(), num_clusters, cluster_cursor.begin());
            for (size_t row = 0; row < rows; ++row)
                for (size_t probe = 0; probe < COARSE_PROBES; ++probe)
                    cluster_rows[cluster_cursor[probes[row * COARSE_PROBES + probe]]++] = static_cast<UInt32>(row);

            for (size_t cluster = 0; cluster < num_clusters; ++cluster)
            {
                const size_t begin = cluster_begin[cluster];
                const size_t count = cluster_begin[cluster + 1] - begin;
                if (count == 0)
                    continue;

                for (size_t i = 0; i < count; ++i)
                {
                    const size_t row = chunk_start + cluster_rows[begin + i];
                    std::copy_n(vec_data + row * dim, dim, gathered_vecs.data() + i * dim);
                    gathered_best[i] = best_score[row];
                    gathered_ids[i] = result_ids[row];
                }
                clusters[cluster].scoreRows(gathered_vecs.data(), count, gathered_best.data(), gathered_ids.data());
                for (size_t i = 0; i < count; ++i)
                {
                    const size_t row = chunk_start + cluster_rows[begin + i];
                    best_score[row] = gathered_best[i];
                    result_ids[row] = gathered_ids[i];
                }
            }
        }
    }
};

using Centroids = std::variant<std::shared_ptr<const CentroidMatrix>, std::shared_ptr<const HierarchicalCentroids>>;

class FunctionAssignCentroid : public IFunction
{
public:
    static constexpr auto name = "assignCentroid";

    explicit FunctionAssignCentroid(ContextPtr context_) : dict_helper(std::move(context_)) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAssignCentroid>(context_); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isDeterministic() const override { return false; } /// dictionary form depends on external, mutable state
    bool isSuitableForConstantFolding() const override { return false; }
    /// Only kicks in when every argument is constant, and `getArgumentsThatAreAlwaysConstant` keeps the
    /// centroids a `ColumnConst` even then, which is what the matrix builder expects.
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments: assignCentroid(vec, centroids | dict_name)", name);

        const auto * vec_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
        if (!vec_type || !isVectorElementType(vec_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be an array of Float32, Float64, BFloat16, Int8 or UInt8", name);

        if (!isCentroidsArray(arguments[1].type) && !isString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a constant array of arrays of Float32, Float64, BFloat16, Int8 or UInt8 "
                "(the centroids) or a constant String (a dictionary name)", name);

        /// Needs recorded use here, while the query is analyzed. Qualifying the name records without loading anything.
        /// Inspected on plans with make_distributed_plan=1 to evaluate dictionary use
        if (const auto * dict_name_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
        {
            const auto & context = dict_helper.getContext();
            [[maybe_unused]] const auto qualified_table_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(dict_name_col->getValue<String>(), context);
        }

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// A distributed initiator can see zero rows with no local copy of the dictionary, so resolving one
        /// here would throw where an empty column is the answer.
        if (input_rows_count == 0)
            return ColumnUInt32::create();

        /// Shared, not owned: the dictionary form hands back the cached centroids, which another thread may
        /// swap, so the refcount is what keeps these alive for the duration of the call.
        Centroids centroids;
        if (isString(arguments[1].type))
        {
            /// Constness is enforced by the framework, see `getArgumentsThatAreAlwaysConstant`.
            centroids = getDictionaryCentroids(assert_cast<const ColumnConst &>(*arguments[1].column).getValue<String>());
        }
        else
        {
            centroids = buildConstMatrix(arguments[1]);
        }

        ColumnPtr vec_full = toFloat32Array(arguments[0]);
        const auto & vec = assert_cast<const ColumnArray &>(*vec_full);
        const auto & vec_data = assert_cast<const ColumnFloat32 &>(vec.getData()).getData();
        const auto & vec_offsets = vec.getOffsets();

        auto result = ColumnUInt32::create(input_rows_count);
        auto & res = result->getData();
        std::visit([&](const auto & impl) { impl->assignBlock(vec_data.data(), vec_offsets, input_rows_count, res); }, centroids);
        return result;
    }

private:
    /// The kernels read Float32. Any other accepted element type is converted once here rather than rejected, so
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
    mutable std::mutex cache_mutex;
    /// A `weak_ptr`, not a raw pointer: expression actions can outlive a query, and comparing raw addresses
    /// is unsafe across a reload - the old dictionary can be destroyed and a later one allocated at the same
    /// address, which would hand back a matrix built from the previous version. A `weak_ptr` expires with the
    /// object it pointed at, so a reused address can never look like a hit. It also does not keep the old
    /// dictionary alive, which a `shared_ptr` here would.
    mutable std::weak_ptr<const IDictionary> cached_dict;
    mutable Centroids cached_centroids;

    static bool isVectorElementType(const DataTypePtr & type) { return isFloat(type) || isInt8(type) || isUInt8(type); }

    static bool isCentroidsArray(const DataTypePtr & type)
    {
        const auto * outer = typeid_cast<const DataTypeArray *>(type.get());
        if (!outer)
            return false;
        const auto * inner = typeid_cast<const DataTypeArray *>(outer->getNestedType().get());
        return inner && isVectorElementType(inner->getNestedType());
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
            size_t start = centroid_index ? inner_array.getOffsets()[static_cast<ssize_t>(centroid_index) - 1] : 0;
            size_t length = inner_array.getOffsets()[centroid_index] - start;
            if (length != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: centroid {} has {} dimensions, expected {}", centroid_index, length, dim);
            std::copy(&values[start], &values[start + length], &row_major[centroid_index * dim]);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(row_major.data(), num_centroids, dim, /*id_values=*/nullptr);
        return matrix;
    }

    /// Read the named dictionary once (columns cid, vec, and level, parent when it has them), cache the result
    /// until the dictionary reloads.
    Centroids getDictionaryCentroids(const String & dict_name) const
    {
        auto dictionary = dict_helper.getDictionary(dict_name);

        /// The lock is held across the read-and-build, not just the lookup.
        std::lock_guard lock(cache_mutex);

        if (cached_dict.lock() == dictionary)
            return cached_centroids;

        const auto & structure = dictionary->getStructure();
        auto has_column = [&](const String & column)
        {
            return structure.hasAttribute(column) || std::ranges::contains(structure.getKeysNames(), column);
        };
        const bool has_level = has_column("level");
        const bool has_parent = has_column("parent");
        if (has_level != has_parent)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "assignCentroid: dictionary {} has a `{}` column but no `{}` column, a two-level dictionary needs both",
                dict_name, has_level ? "level" : "parent", has_level ? "parent" : "level");
        const bool two_level = has_level;

        Names columns{"cid", "vec"};
        if (two_level)
            columns.insert(columns.end(), {"level", "parent"});

        /// Full-read the dictionary (same mechanism the dictionary() table function uses).
        QueryPipeline pipeline(dictionary->read(columns, /*max_block_size=*/65536, /*num_streams=*/1));
        PullingPipelineExecutor executor(pipeline);

        struct DictionaryCentroid
        {
            UInt64 cid;
            UInt64 level;
            UInt64 parent;
            VectorWithMemoryTracking<Float32> vec;
        };
        VectorWithMemoryTracking<DictionaryCentroid> centroids;
        Block block;
        while (executor.pull(block))
        {
            /// `getUInt` accepts any arithmetic column, so a `Float64` or `Int64` key would be silently cast
            /// and we would hand back an id the dictionary never stored. Check the type, not just the range.
            auto get_uint_column = [&](const String & column) -> const IColumn &
            {
                const auto & with_type = block.getByName(column);
                if (!WhichDataType(with_type.type).isNativeUInt())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "assignCentroid: attribute `{}` of dictionary {} must be an unsigned integer, got {}",
                        column, dict_name, with_type.type->getName());
                return *with_type.column;
            };
            const IColumn & cid_col = get_uint_column("cid");
            const IColumn * level_col = two_level ? &get_uint_column("level") : nullptr;
            const IColumn * parent_col = two_level ? &get_uint_column("parent") : nullptr;
            const auto & vec_col = block.getByName("vec");

            /// The dictionary type is only known here (the name is a runtime string), and the kernel below reads
            /// the nested column as ColumnFloat32, so reject anything else instead of reinterpreting the payload.
            const auto * vec_type = typeid_cast<const DataTypeArray *>(vec_col.type.get());
            if (!vec_type || !WhichDataType(vec_type->getNestedType()).isFloat32())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "assignCentroid: attribute `vec` of dictionary {} must be Array(Float32), got {}",
                    dict_name, vec_col.type->getName());

            const auto & vec_arr = assert_cast<const ColumnArray &>(*vec_col.column);
            const auto & vec_vals = assert_cast<const ColumnFloat32 &>(vec_arr.getData()).getData();
            const auto & vec_off = vec_arr.getOffsets();
            for (size_t row = 0; row < cid_col.size(); ++row)
            {
                size_t start = row ? vec_off[row - 1] : 0;
                size_t length = vec_off[row] - start;
                centroids.push_back({
                    cid_col.getUInt(row),
                    level_col ? level_col->getUInt(row) : 0,
                    parent_col ? parent_col->getUInt(row) : 0,
                    VectorWithMemoryTracking<Float32>(&vec_vals[start], &vec_vals[start + length])});
            }
        }

        if (centroids.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} produced no centroids", dict_name);

        std::sort(centroids.begin(), centroids.end(), [](const auto & a, const auto & b) { return a.cid < b.cid; });

        size_t dim = centroids[0].vec.size();
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} has zero-dimension centroids", dict_name);

        for (const auto & centroid : centroids)
        {
            if (centroid.vec.size() != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: dictionary {} centroid {} has {} dimensions, expected {}",
                    dict_name, centroid.cid, centroid.vec.size(), dim);

            /// The result type is exactly UInt32 - reject anything greater
            if (centroid.cid > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: dictionary {} has cid {} which exceeds the UInt32 range of the result",
                    dict_name, centroid.cid);
        }

        /// The centroids at `rows` (positions in `centroids`), with their cids as ids or, if not `with_ids`, 0..n-1.
        auto build_matrix = [&](const VectorWithMemoryTracking<size_t> & rows, bool with_ids)
        {
            VectorWithMemoryTracking<Float32> row_major(rows.size() * dim);
            VectorWithMemoryTracking<UInt32> ids(rows.size());
            for (size_t i = 0; i < rows.size(); ++i)
            {
                const auto & centroid = centroids[rows[i]];
                std::copy(centroid.vec.begin(), centroid.vec.end(), &row_major[i * dim]);
                ids[i] = static_cast<UInt32>(centroid.cid);
            }
            CentroidMatrix matrix;
            matrix.build(row_major.data(), rows.size(), dim, with_ids ? ids.data() : nullptr);
            return matrix;
        };

        Centroids result;
        if (!two_level)
        {
            VectorWithMemoryTracking<size_t> all_rows(centroids.size());
            std::iota(all_rows.begin(), all_rows.end(), 0);
            result = std::make_shared<const CentroidMatrix>(build_matrix(all_rows, /*with_ids=*/true));
        }
        else
        {
            /// Level 1 rows are the coarse centroids, in ascending cid as `centroids` is sorted.
            VectorWithMemoryTracking<size_t> coarse_rows;
            for (size_t i = 0; i < centroids.size(); ++i)
            {
                const auto & centroid = centroids[i];
                if (centroid.level > 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "assignCentroid: dictionary {} centroid {} has level {}, only levels 0 (leaf) and 1 (coarse) are supported",
                        dict_name, centroid.cid, centroid.level);
                if (centroid.level == 0)
                    continue;
                /// Only a complex key can repeat a cid, and then `parent` would not name one centroid.
                if (!coarse_rows.empty() && centroids[coarse_rows.back()].cid == centroid.cid)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "assignCentroid: dictionary {} has more than one level 1 centroid with cid {}", dict_name, centroid.cid);
                coarse_rows.push_back(i);
            }

            VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> leaf_rows(coarse_rows.size());
            bool has_leaves = false;
            for (size_t i = 0; i < centroids.size(); ++i)
            {
                const auto & centroid = centroids[i];
                if (centroid.level != 0)
                    continue;
                const auto coarse = std::ranges::lower_bound(
                    coarse_rows, centroid.parent, {}, [&](size_t row) { return centroids[row].cid; });
                if (coarse == coarse_rows.end() || centroids[*coarse].cid != centroid.parent)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "assignCentroid: dictionary {} leaf centroid {} has parent {}, which is not the cid of a level 1 centroid",
                        dict_name, centroid.cid, centroid.parent);
                leaf_rows[coarse - coarse_rows.begin()].push_back(i);
                has_leaves = true;
            }
            if (!has_leaves)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} has no level 0 (leaf) centroids", dict_name);

            /// A coarse centroid without leaves (k-means can leave one) is dropped, so every probe opens a cluster.
            auto hierarchical = std::make_shared<HierarchicalCentroids>();
            hierarchical->dim = dim;
            VectorWithMemoryTracking<size_t> non_empty_coarse_rows;
            for (size_t c = 0; c < coarse_rows.size(); ++c)
            {
                if (leaf_rows[c].empty())
                    continue;
                non_empty_coarse_rows.push_back(coarse_rows[c]);
                hierarchical->clusters.push_back(build_matrix(leaf_rows[c], /*with_ids=*/true));
            }
            hierarchical->coarse = build_matrix(non_empty_coarse_rows, /*with_ids=*/false);
            result = std::move(hierarchical);
        }

        cached_centroids = result;
        cached_dict = dictionary;
        return result;
    }
};

}

REGISTER_FUNCTION(AssignCentroid)
{
    FunctionDocumentation::Description description =
        "Returns the id of the nearest (L2) centroid to a vector. The centroids are given as a constant "
        "array of arrays, where the id is the 0-based position in that array, or as the name of a "
        "`Dictionary` holding the attributes `cid` and `vec`, where the id is `cid`.\n\n"
        "If the dictionary also has the columns `level` and `parent`, the search is two-level and approximate:\n\n"
        "- Rows with `level` 0 are leaf centroids. Only their ids are returned.\n"
        "- Rows with `level` 1 are coarse centroids. The `parent` of a leaf is the `cid` of its coarse centroid.\n"
        "- The vector is compared with every coarse centroid, then only with the leaves of the 16 nearest ones.\n"
        "- The result is the nearest of those leaves. It is the nearest leaf overall when its coarse centroid is among "
        "the 16, and always when at most 16 coarse centroids have leaves.\n"
        "- Coarse centroids without leaves are ignored.\n\n"
        "Only one of `level` and `parent`, a `level` other than 0 or 1, a leaf whose `parent` is not the `cid` of a "
        "coarse centroid, no leaves, or two coarse centroids with the same `cid` is an error.";
    FunctionDocumentation::Syntax syntax = "assignCentroid(vec, centroids | dict_name)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Vector to assign. Its dimension must match the dimension of the centroids. Element types other than "
                "`Float32` are converted to `Float32`, which is what the scoring kernel uses.",
         {"Array(Float32)", "Array(Float64)", "Array(BFloat16)", "Array(Int8)", "Array(UInt8)"}},
        {"centroids", "The centroids to score against, which must be constant. Given as an array of equally sized, "
                      "non-empty arrays with any of the element types accepted for `vec` (not necessarily the same "
                      "one), the id then being the 0-based position in that array; or as the name of a `Dictionary` "
                      "with an attribute `cid` of an unsigned integer type that fits `UInt32` and an attribute `vec` "
                      "of type `Array(Float32)`, the id then being `cid`. Optional columns `level` and `parent` of "
                      "unsigned integer types select the two-level search. The dictionary is read once and cached "
                      "until it reloads.",
         {"Array(Array(Float32))", "Array(Array(Float64))", "Array(Array(BFloat16))", "Array(Array(Int8))",
          "Array(Array(UInt8))", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The nearest centroid id.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Inline centroids",
         "SELECT assignCentroid([1.0, 2.0]::Array(Float32), [[0.0, 0.0], [1.0, 2.0]]::Array(Array(Float32)))", "1"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAssignCentroid>(documentation);
}

}
