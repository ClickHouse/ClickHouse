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

/// Register-blocking shape: 6 x 16 accumulators is 12 YMM registers on AVX2 (of 16), leaving the rest for
/// operands. They must stay in registers - an L1-resident accumulator costs a load and a store per FMA.
/// See https://en.wikipedia.org/wiki/Loop_nest_optimization
constexpr size_t ROW_BLOCK = 6;
constexpr size_t COL_BLOCK = 16;

DECLARE_MULTITARGET_CODE(

/// Score n rows against ONE packed tile of width centroids, keeping the running best score/id per row.
/// First check build() and assignBlock() to understand how the initial preparation is done.
///
/// A blocked GEMM rather than a sequence of GEMVs: scoring one row at a time re-reads the whole centroid tile
/// per vector, which at 32768 x 768 (~100 MB) is bandwidth-bound; blocking ROW_BLOCK rows cut it 3.5x.
/// See https://en.wikipedia.org/wiki/Matrix_multiplication_algorithm (the cache-blocked algorithm), or
/// Goto & van de Geijn, "Anatomy of High-Performance Matrix Multiplication", ACM TOMS 34(3), 2008, which is
/// where this microkernel shape comes from.
void scoreTile(
    const Float32 * __restrict vec_data, size_t n, size_t dim,
    const Float32 * __restrict pack, const Float32 * __restrict cnorm_tile, const UInt32 * __restrict ids_tile,
    size_t width, Float32 * __restrict best_score, UInt32 * __restrict res)
{
    /// Compute the score for this row and update best so far
    auto reduce_row = [&](size_t row, const Float32 * __restrict a, size_t c0, size_t count)
    {
        Float32 bs = best_score[row];
        UInt32 bid = res[row];
        for (size_t c = 0; c < count; ++c)
        {
            const Float32 score = cnorm_tile[c0 + c] - 2.0f * a[c];
            if (score < bs)
            {
                bs = score;
                bid = ids_tile[c0 + c];
            }
        }
        best_score[row] = bs;
        res[row] = bid;
    };

    size_t row = 0;
    for (; row + ROW_BLOCK <= n; row += ROW_BLOCK) /// 6 incoming vectors at a time
    {
        for (size_t c0 = 0; c0 < width; c0 += COL_BLOCK) /// 16 centroids at a time
        {
            Float32 acc[ROW_BLOCK][COL_BLOCK] = {};

            for (size_t j = 0; j < dim; ++j) /// one per dimension
            {
                const Float32 * __restrict col = pack + j * width + c0;
                for (size_t r = 0; r < ROW_BLOCK; ++r)
                {
                    const Float32 xj = vec_data[(row + r) * dim + j];
                    for (size_t c = 0; c < COL_BLOCK; ++c)
                        acc[r][c] += xj * col[c];
                }
            }

            for (size_t r = 0; r < ROW_BLOCK; ++r)
                reduce_row(row + r, acc[r], c0, COL_BLOCK);
        }
    }

    /// Tail rows that do not fill a block.
    for (; row < n; ++row)
    {
        for (size_t c0 = 0; c0 < width; c0 += COL_BLOCK)
        {
            Float32 acc[COL_BLOCK] = {};

            for (size_t j = 0; j < dim; ++j)
            {
                const Float32 xj = vec_data[row * dim + j];
                const Float32 * __restrict col = pack + j * width + c0;
                for (size_t c = 0; c < COL_BLOCK; ++c)
                    acc[c] += xj * col[c];
            }

            reduce_row(row, acc, c0, COL_BLOCK);
        }
    }
}

) // DECLARE_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is off (ARM, or
/// ENABLE_MULTITARGET_CODE=OFF) only Default exists, hence the plain loops above that auto-vectorize.
/// SIMD only, deliberately: executeImpl already runs on one pipeline thread per block, so threading here
/// would only oversubscribe.
void scoreTile(
    const Float32 * vec_data, size_t n, size_t dim,
    const Float32 * pack, const Float32 * cnorm_tile, const UInt32 * ids_tile,
    size_t width, Float32 * best_score, UInt32 * res)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, best_score, res);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, best_score, res);
        return;
    }
#endif
    TargetSpecific::Default::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, best_score, res);
}

}
}

namespace
{

/// Coordinates are squared and summed in Float32, so a finite but very large value can still overflow the
/// accumulator to infinity, making `score = cnorm - 2 * dot` a NaN. No `score < best` comparison is then
/// true and the row silently takes the fallback id. Bounding `|x|` by `sqrt(FLT_MAX / (4 * dim))` keeps the
/// sum of squares, the dot product and their difference finite. At dim = 768 that is ~3.3e17, far above any
/// real embedding, so this rejects only input the kernel could not have scored correctly anyway.
Float32 coordinateLimit(size_t dim)
{
    return static_cast<Float32>(
        std::sqrt(static_cast<double>(std::numeric_limits<Float32>::max()) / (4.0 * static_cast<double>(dim))));
}

/// Column-major centroids + squared norms + the id to return per centroid.
struct CentroidMatrix
{
    size_t k = 0;
    size_t dim = 0;

    /// ct is the k centroids laid out in column-major form : ct[j * k + c] = coordinate j of centroid c
    VectorWithMemoryTracking<Float32> ct;

    VectorWithMemoryTracking<Float32> cnorm;   /// ||c||^2 - squared norm of the k centroids
    VectorWithMemoryTracking<UInt32> ids;      /// cluster id returned when centroid c is nearest

    /// Packs the centroids into the layout the kernel reads. `rows` is row-major (k * dim); `id_values` gives
    /// the id per centroid, or null for 0..k-1. Runs once per block for the inline form and once per
    /// dictionary version for the cached dictionary form - never per row.
    ///
    /// For k = 3 centroids of dim = 2, rows = [[1,2], [3,4], [5,6]]:
    ///
    ///     ct    = [1, 3, 5,  2, 4, 6]   coordinate 0 of every centroid, then coordinate 1
    ///     cnorm = [5, 25, 61]           1*1+2*2, 3*3+4*4, 5*5+6*6
    ///     ids   = [0, 1, 2]             or the dictionary cids when id_values is given
    void build(const Float32 * rows, size_t k_, size_t dim_, const UInt32 * id_values)
    {
        k = k_;
        dim = dim_;
        ct.assign(dim * k, 0.0f);
        cnorm.assign(k, 0.0f);
        ids.resize(k);
        const Float32 limit = coordinateLimit(dim);
        for (size_t c = 0; c < k; ++c)
        {
            const Float32 * cen = rows + c * dim;
            double s = 0;
            for (size_t j = 0; j < dim; ++j)
            {
                /// A centroid the kernel cannot score is silently unreachable rather than an error, so both
                /// checks belong here. Free: this loop already reads every coordinate to build the norm.
                if (!std::isfinite(cen[j]))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} must not contain non-finite values (NaN or Inf)", c);
                if (std::abs(cen[j]) > limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "assignCentroid: centroid {} has coordinate {}, above the largest magnitude the "
                        "Float32 scoring math can represent for dimension {} ({})", c, cen[j], dim, limit);
                ct[j * k + c] = cen[j];
                s += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);
            }
            cnorm[c] = static_cast<Float32>(s);
            ids[c] = id_values ? id_values[c] : static_cast<UInt32>(c);
        }
    }

    /// Assign every vector in a block to its nearest-centroid id, writing ids into res (already sized n).
    /// Worked example : a block has three rows: [[1,2], [3,4,5], [6]]. ClickHouse stores:
    /// vec_data = [1, 2, 3, 4, 5, 6]     ← every row's floats, concatenated
    /// offsets  = [2, 5, 6]              ← where each row ENDS (exclusive)
    /// vec_data is the flat float buffer for the whole block — all rows vectors laid end to end
    /// offsets is one number per row: the end position of that row's slice.
    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t n, PaddedPODArray<UInt32> & res) const
    {
        /// Both builders reject an empty or zero-dimension centroid set, so either being zero here is a
        /// programming error rather than bad input. Stated explicitly because it is a real precondition -
        /// `dim` divides the tile size below - and because it does not survive the call boundary otherwise.
        if (k == 0 || dim == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "assignCentroid: centroid matrix is empty (k = {}, dim = {})", k, dim);

        for (size_t row = 0; row < n; ++row) /// validate dimensions up front - the hot loop in GEMM needs that
        {
            size_t start = row ? offsets[row - 1] : 0;
            size_t len = offsets[row] - start;
            if (len != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: input vector has {} dimensions but centroids have {}", len, dim);
        }

        /// A NaN probe never satisfies `score < bs`, so it would fall through to the `ids[0]` fallback and
        /// return a plausible-looking id instead of failing. Swept linearly - the check above has established
        /// that the rows are dense, so the payload is exactly `n * dim` floats.
        const Float32 limit = coordinateLimit(dim);
        for (size_t i = 0; i < n * dim; ++i)
        {
            if (!std::isfinite(vec_data[i]))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input vector must not contain non-finite values (NaN or Inf)");
            if (std::abs(vec_data[i]) > limit)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "assignCentroid: input coordinate {} is above the largest magnitude the Float32 scoring "
                    "math can represent for dimension {} ({})", vec_data[i], dim, limit);
        }

        VectorWithMemoryTracking<Float32> best_score(n, std::numeric_limits<Float32>::max());
        for (size_t row = 0; row < n; ++row)
            res[row] = ids[0];

        /// Worked example to understand a TILE :
        /// With k=32768 and dim=768, the full centroid matrix is 32768 × 768 × 4 B = 100 MB.
        /// Every input vector must be compared against all of it. Sweep the whole 100 MB once
        /// per vector and you're reading from DRAM the entire time.
        /// So instead: grab 'tile' centroids, score all n rows against that slice, then move to
        /// the next slice. The slice is small enough to live in L2 cache. Note the tile is about
        /// L2 cache and ROW_BLOCK / COL_BLOCK in another routine is about registers.
        ///
        /// tile = 512 KB ÷ (768 × 4 B) = 170, rounded down to a multiple of 16 → 160 centroids per slice
        static constexpr size_t L2_TILE_BYTES = 512 * 1024;
        constexpr size_t CB = AssignCentroidImpl::COL_BLOCK;
        const size_t tile = std::clamp<size_t>(
            (L2_TILE_BYTES / (dim * sizeof(Float32))) / CB * CB, CB, 1024);

        VectorWithMemoryTracking<Float32> pack(tile * dim);
        VectorWithMemoryTracking<Float32> cnorm_tile(tile);
        VectorWithMemoryTracking<UInt32> ids_tile(tile);

        /// Note the tile increment. This loop will run for 32768/160 = 205 times for the example.
        for (size_t c0 = 0; c0 < k; c0 += tile)
        {
            const size_t width = std::min(tile, k - c0);
            const size_t padded = (width + CB - 1) / CB * CB;

            for (size_t j = 0; j < dim; ++j)
            {
                Float32 * pack_row = pack.data() + j * padded;

                /// ct has been laid out in column major form in build(). We will copy
                /// 160 floats (1 dimension of each of the 160 centroids in the tile)
                std::copy(&ct[j * k + c0], &ct[j * k + c0] + width, pack_row);

                std::fill(pack_row + width, pack_row + padded, 0.0f); /// if any padding
            }
            /// Lay out the squared-norms and ids also for tile
            std::copy(&cnorm[c0], &cnorm[c0] + width, cnorm_tile.begin());
            std::fill(cnorm_tile.begin() + width, cnorm_tile.begin() + padded, std::numeric_limits<Float32>::infinity());
            std::copy(&ids[c0], &ids[c0] + width, ids_tile.begin());
            std::fill(ids_tile.begin() + width, ids_tile.begin() + padded, 0u);

            /// Example: if vec_data is an INSERT block of 1000 vectors, we will score the 1000
            /// against 160 centroids in each iteration and keep track in best_score & res
            AssignCentroidImpl::scoreTile(
                vec_data, n, dim, pack.data(), cnorm_tile.data(), ids_tile.data(), padded,
                best_score.data(), res.data());
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

        const auto & outer = assert_cast<const ColumnArray &>(*casted);                    /// one row = the k centroids
        const auto & inner = assert_cast<const ColumnArray &>(outer.getData());            /// k inner arrays
        const auto & values = assert_cast<const ColumnFloat32 &>(inner.getData()).getData();

        size_t k = outer.getOffsets()[0];
        if (k == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: centroids array is empty");

        size_t dim = inner.getOffsets()[0];
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: centroids have zero dimension");

        VectorWithMemoryTracking<Float32> row_major(k * dim);
        for (size_t c = 0; c < k; ++c)
        {
            size_t start = c ? inner.getOffsets()[c - 1] : 0;
            size_t len = inner.getOffsets()[c] - start;
            if (len != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: centroid {} has {} dimensions, expected {}", c, len, dim);
            std::copy(&values[start], &values[start + len], &row_major[c * dim]);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(row_major.data(), k, dim, /*id_values=*/nullptr);
        return matrix;
    }

    /// Read the named dictionary once (columns cid, vec), cache the matrix until the dictionary reloads.
    std::shared_ptr<const CentroidMatrix> getDictionaryMatrix(const String & dict_name) const
    {
        auto dictionary = dict_helper.getDictionary(dict_name);

        {
            std::lock_guard lock(cache_mutex);
            if (cached_matrix && cached_dict.lock() == dictionary)
                return cached_matrix;
        }

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
            for (size_t i = 0; i < cid_col->size(); ++i)
            {
                size_t start = i ? vec_off[i - 1] : 0;
                size_t len = vec_off[i] - start;
                centroids.emplace_back(cid_col->getUInt(i), VectorWithMemoryTracking<Float32>(&vec_vals[start], &vec_vals[start + len]));
            }
        }

        if (centroids.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} produced no centroids", dict_name);

        std::sort(centroids.begin(), centroids.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

        size_t k = centroids.size();
        size_t dim = centroids[0].second.size();
        if (dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "assignCentroid: dictionary {} has zero-dimension centroids", dict_name);

        VectorWithMemoryTracking<Float32> row_major(k * dim);
        VectorWithMemoryTracking<UInt32> ids(k);
        for (size_t c = 0; c < k; ++c)
        {
            if (centroids[c].second.size() != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: dictionary {} centroid {} has {} dimensions, expected {}",
                    dict_name, centroids[c].first, centroids[c].second.size(), dim);
            std::copy(centroids[c].second.begin(), centroids[c].second.end(), &row_major[c * dim]);

            /// The result type is exactly UInt32 - reject anything greater
            if (centroids[c].first > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "assignCentroid: dictionary {} has cid {} which exceeds the UInt32 range of the result",
                    dict_name, centroids[c].first);
            ids[c] = static_cast<UInt32>(centroids[c].first);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(row_major.data(), k, dim, ids.data());

        {
            std::lock_guard lock(cache_mutex);
            cached_matrix = matrix;
            cached_dict = dictionary;
        }
        return matrix;
    }
};

}

REGISTER_FUNCTION(AssignCentroid)
{
    FunctionDocumentation::Description description =
        "Returns the id of the nearest (L2) centroid to a vector. The centroids are given as a constant "
        "array of float arrays, where the id is the 0-based position in that array, or as the name of a "
        "Dictionary holding columns (cid, vec), where the id is the cid.";
    FunctionDocumentation::Syntax syntax = "assignCentroid(vec, centroids | dict_name)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Input vector.", {"Array(Float32)", "Array(Float64)", "Array(BFloat16)"}},
        {"centroids", "Constant centroids as Array(Array(Float32)), or a constant dictionary name.", {"Array(Array(Float32))", "String"}}
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
