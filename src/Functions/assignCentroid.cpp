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
#include <Common/assert_cast.h>
#include <Common/TargetSpecific.h>
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

/// Column-major centroids + squared norms + the id to return per centroid.
struct CentroidMatrix
{
    size_t num_centroids = 0;
    size_t dim = 0;

    /// Column-major: `centroids_transposed[coord * num_centroids + c]` is coordinate `coord` of centroid `c`.
    VectorWithMemoryTracking<Float32> centroids_transposed;

    VectorWithMemoryTracking<Float32> centroid_sq_norms;   /// the squared norm of each centroid
    VectorWithMemoryTracking<UInt32> ids;                  /// the id to return when that centroid is nearest

    /// Pack the centroids into the layout the kernel reads. `id_values` gives the id per centroid, or null
    /// to use 0..num_centroids-1. Runs once per block for the inline form, and once per dictionary version
    /// for the dictionary form - never per row.
    ///
    /// For three centroids of dimension 2, `row_major` = [[1,2], [3,4], [5,6]]:
    ///
    ///     centroids_transposed = [1, 3, 5,  2, 4, 6]   coordinate 0 of every centroid, then coordinate 1
    ///     centroid_sq_norms    = [5, 25, 61]           1*1+2*2, 3*3+4*4, 5*5+6*6
    ///     ids                  = [0, 1, 2]             or the dictionary cids when id_values is given
    void build(const Float32 * row_major, size_t num_centroids_, size_t dim_, const UInt32 * id_values)
    {
        num_centroids = num_centroids_;
        dim = dim_;
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
                centroids_transposed[coord * num_centroids + centroid_index] = centroid[coord];
                sq_norm += static_cast<double>(centroid[coord]) * static_cast<double>(centroid[coord]);
            }
            centroid_sq_norms[centroid_index] = static_cast<Float32>(sq_norm);
            ids[centroid_index] = id_values ? id_values[centroid_index] : static_cast<UInt32>(centroid_index);
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
};

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments: assignCentroid(vec, centroids | dict_name)", name);

        const auto * vec_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!vec_type || !isFloat(vec_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be an array of floats", name);

        if (!isCentroidsArray(arguments[1]) && !isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a constant array of float arrays (the centroids) "
                "or a constant String (a dictionary name)", name);

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// A distributed initiator can see zero rows with no local copy of the dictionary, so resolving one
        /// here would throw where an empty column is the answer.
        if (input_rows_count == 0)
            return ColumnUInt32::create();

        /// Shared, not owned: the dictionary form hands back the cached matrix, which another thread may
        /// swap, so the refcount is what keeps this one alive for the duration of the call.
        std::shared_ptr<const CentroidMatrix> matrix;
        if (isString(arguments[1].type))
        {
            /// Constness is enforced by the framework, see `getArgumentsThatAreAlwaysConstant`.
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
    mutable std::mutex cache_mutex;
    /// A `weak_ptr`, not a raw pointer: expression actions can outlive a query, and comparing raw addresses
    /// is unsafe across a reload - the old dictionary can be destroyed and a later one allocated at the same
    /// address, which would hand back a matrix built from the previous version. A `weak_ptr` expires with the
    /// object it pointed at, so a reused address can never look like a hit. It also does not keep the old
    /// dictionary alive, which a `shared_ptr` here would.
    mutable std::weak_ptr<const IDictionary> cached_dict;
    mutable std::shared_ptr<const CentroidMatrix> cached_matrix;

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
        matrix->build(row_major.data(), num_centroids, dim, /*id_values=*/nullptr);
        return matrix;
    }

    /// Read the named dictionary once (columns cid, vec), cache the matrix until the dictionary reloads.
    std::shared_ptr<const CentroidMatrix> getDictionaryMatrix(const String & dict_name) const
    {
        auto dictionary = dict_helper.getDictionary(dict_name);

        /// The lock is held across the read-and-build, not just the lookup.
        std::lock_guard lock(cache_mutex);

        if (cached_matrix && cached_dict.lock() == dictionary)
            return cached_matrix;

        /// Full-read the dictionary (same mechanism the dictionary() table function uses).
        QueryPipeline pipeline(dictionary->read(Names{"cid", "vec"}, /*max_block_size=*/65536, /*num_streams=*/1));
        PullingPipelineExecutor executor(pipeline);

        VectorWithMemoryTracking<std::pair<UInt64, VectorWithMemoryTracking<Float32>>> centroids;
        Block block;
        while (executor.pull(block))
        {
            const auto & cid_with_type = block.getByName("cid");

            /// `getUInt` accepts any arithmetic column, so a `Float64` or `Int64` key would be silently cast
            /// and we would hand back an id the dictionary never stored. Check the type, not just the range.
            if (!WhichDataType(cid_with_type.type).isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "assignCentroid: attribute `cid` of dictionary {} must be an unsigned integer, got {}",
                    dict_name, cid_with_type.type->getName());

            const auto & cid_col = cid_with_type.column;
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
            for (size_t row = 0; row < cid_col->size(); ++row)
            {
                size_t start = row ? vec_off[row - 1] : 0;
                size_t length = vec_off[row] - start;
                centroids.emplace_back(cid_col->getUInt(row), VectorWithMemoryTracking<Float32>(&vec_vals[start], &vec_vals[start + length]));
            }
        }

        if (centroids.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} produced no centroids", dict_name);

        std::sort(centroids.begin(), centroids.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

        size_t num_centroids = centroids.size();
        size_t dim = centroids[0].second.size();
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} has zero-dimension centroids", dict_name);

        VectorWithMemoryTracking<Float32> row_major(num_centroids * dim);
        VectorWithMemoryTracking<UInt32> ids(num_centroids);
        for (size_t centroid_index = 0; centroid_index < num_centroids; ++centroid_index)
        {
            if (centroids[centroid_index].second.size() != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: dictionary {} centroid {} has {} dimensions, expected {}",
                    dict_name, centroids[centroid_index].first, centroids[centroid_index].second.size(), dim);
            std::copy(centroids[centroid_index].second.begin(), centroids[centroid_index].second.end(), &row_major[centroid_index * dim]);

            /// The result type is exactly UInt32 - reject anything greater
            if (centroids[centroid_index].first > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: dictionary {} has cid {} which exceeds the UInt32 range of the result",
                    dict_name, centroids[centroid_index].first);
            ids[centroid_index] = static_cast<UInt32>(centroids[centroid_index].first);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(row_major.data(), num_centroids, dim, ids.data());

        cached_matrix = matrix;
        cached_dict = dictionary;
        return matrix;
    }
};

}

REGISTER_FUNCTION(AssignCentroid)
{
    FunctionDocumentation::Description description =
        "Returns the id of the nearest (L2) centroid to a vector. The centroids are given as a constant "
        "array of float arrays, where the id is the 0-based position in that array, or as the name of a "
        "`Dictionary` holding the attributes `cid` and `vec`, where the id is `cid`.";
    FunctionDocumentation::Syntax syntax = "assignCentroid(vec, centroids | dict_name)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Vector to assign. Its dimension must match the dimension of the centroids. Widths other than "
                "`Float32` are converted to `Float32`, which is what the scoring kernel uses.",
         {"Array(Float32)", "Array(Float64)", "Array(BFloat16)"}},
        {"centroids", "The centroids to score against, which must be constant. Given as an array of equally sized, "
                      "non-empty float arrays, the id then being the 0-based position in that array; or as the name of "
                      "a `Dictionary` with an attribute `cid` of an unsigned integer type that fits `UInt32` and an "
                      "attribute `vec` of type `Array(Float32)`, the id then being `cid`. The dictionary is read once "
                      "and cached until it reloads.",
         {"Array(Array(Float32))", "Array(Array(Float64))", "Array(Array(BFloat16))", "String"}}
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
