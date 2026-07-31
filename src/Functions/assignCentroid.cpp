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
#include <Dictionaries/IDictionary.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Common/assert_cast.h>
#include <Common/TargetSpecific.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

/// `assignCentroid(vec, centroids)` -> UInt32 cluster id: the index of the nearest (L2) centroid to `vec`.
///
/// The second argument is a CONSTANT and may be either:
///   * `Array(Array(Float32))` - the centroids inline. The returned id is the position in this array
///     (matching the usual `arrayJoin(hierarchicalKMeans(...))` + `rowNumberInAllBlocks()` convention).
///   * `String` - the name of a Dictionary holding columns (`cid` UInt*, `vec` Array(Float32)). The centroids
///     are read once and cached (per dictionary version); the returned id is the dictionary's `cid`.
///
/// Both forms share one kernel. The centroids are materialized into a column-major matrix ONCE per call
/// (from the const value, or from the cached dictionary read), then every row in the block is scored against
/// all centroids via the reformulation argmin_c ||x - c||^2 = argmin_c(||c||^2 - 2 x.c). Because the second
/// argument is a `ColumnConst`, the matrix is never broadcast per row (unlike an `arrayMap` over a const array
/// in SQL, which materializes the array for every row and blows up memory).

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

/// Named (not anonymous) so the `TargetSpecific::*` namespaces the macro generates cannot collide with
/// identically named kernels from another translation unit.
namespace AssignCentroidImpl
{
namespace
{

DECLARE_MULTITARGET_CODE(

/// Score `n` rows against ONE packed tile of `width` centroids and keep the running best score/id per row.
/// `pack` is column-major within the tile (`pack[j * width + c]`), so the inner loop over centroids is a
/// contiguous FMA against a broadcast `x[j]` - the shape the vectorizer wants. `acc` is caller-provided
/// scratch of at least `width` floats (kept off the stack because `width` is up to the tile size).
///
/// All rows are known to have exactly `dim` elements (validated by the caller), so row `r` starts at
/// `r * dim` and the offsets array does not need to be touched in the hot loop.
void scoreTile(
    const Float32 * __restrict vec_data, size_t n, size_t dim,
    const Float32 * __restrict pack, const Float32 * __restrict cnorm_tile, const UInt32 * __restrict ids_tile,
    size_t width, Float32 * __restrict acc, Float32 * __restrict best_score, UInt32 * __restrict res)
{
    for (size_t row = 0; row < n; ++row)
    {
        const Float32 * __restrict x = vec_data + row * dim;

        for (size_t c = 0; c < width; ++c)
            acc[c] = 0.0f;

        for (size_t j = 0; j < dim; ++j)
        {
            const Float32 xj = x[j];
            const Float32 * __restrict col = pack + j * width;
            for (size_t c = 0; c < width; ++c)
                acc[c] += xj * col[c];
        }

        Float32 bs = best_score[row];
        UInt32 bid = res[row];
        for (size_t c = 0; c < width; ++c)
        {
            const Float32 score = cnorm_tile[c] - 2.0f * acc[c];
            if (score < bs)
            {
                bs = score;
                bid = ids_tile[c];
            }
        }
        best_score[row] = bs;
        res[row] = bid;
    }
}

) // DECLARE_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is disabled (ARM, and any
/// build with `ENABLE_MULTITARGET_CODE=OFF`) only `Default` exists, which is why the kernel above is written
/// as plain contiguous loops the compiler can auto-vectorize on its own.
///
/// Note this is SIMD only, deliberately: `executeImpl` already runs concurrently on many pipeline threads,
/// one per block, so spawning threads inside it would just oversubscribe.
void scoreTile(
    const Float32 * vec_data, size_t n, size_t dim,
    const Float32 * pack, const Float32 * cnorm_tile, const UInt32 * ids_tile,
    size_t width, Float32 * acc, Float32 * best_score, UInt32 * res)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, acc, best_score, res);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, acc, best_score, res);
        return;
    }
#endif
    TargetSpecific::Default::scoreTile(vec_data, n, dim, pack, cnorm_tile, ids_tile, width, acc, best_score, res);
}

}
}

namespace
{

/// Column-major centroids + squared norms + the id to return per centroid.
struct CentroidMatrix
{
    size_t k = 0;
    size_t dim = 0;
    VectorWithMemoryTracking<Float32> ct;      /// column-major: ct[j * k + c] = coordinate j of centroid c
    VectorWithMemoryTracking<Float32> cnorm;   /// ||c||^2
    VectorWithMemoryTracking<UInt32> ids;      /// cluster id returned when centroid c is nearest

    /// `rows` is row-major (k * dim). `id_values` (optional) gives the id per centroid; default is 0..k-1.
    void build(const Float32 * rows, size_t k_, size_t dim_, const UInt32 * id_values)
    {
        k = k_;
        dim = dim_;
        ct.assign(dim * k, 0.0f);
        cnorm.assign(k, 0.0f);
        ids.resize(k);
        for (size_t c = 0; c < k; ++c)
        {
            const Float32 * cen = rows + c * dim;
            double s = 0;
            for (size_t j = 0; j < dim; ++j)
            {
                ct[j * k + c] = cen[j];
                s += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);
            }
            cnorm[c] = static_cast<Float32>(s);
            ids[c] = id_values ? id_values[c] : static_cast<UInt32>(c);
        }
    }

    /// Assign every vector in a block to its nearest-centroid id, writing ids into `res` (already sized `n`).
    ///
    /// Tiled over centroids: for each tile of `tile` centroids we pack them contiguously once, then score ALL `n`
    /// vectors against that cache-resident tile before moving on. This streams the (multi-MB) centroid matrix from
    /// memory ONCE per block instead of once per vector - the naive per-vector loop re-reads all `k * dim` floats
    /// for every row, which makes a large-`k` scan memory-bandwidth-bound and kills thread scaling. Same math as
    /// before: argmin_c ||x - c||^2 = argmin_c(||c||^2 - 2 x.c).
    void assignBlock(const Float32 * vec_data, const ColumnArray::Offsets & offsets, size_t n, PaddedPODArray<UInt32> & res) const
    {
        for (size_t row = 0; row < n; ++row) /// validate dimensions up front
        {
            size_t start = row ? offsets[row - 1] : 0;
            size_t len = offsets[row] - start;
            if (len != dim)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "assignCentroid: input vector has {} dimensions but centroids have {}", len, dim);
        }

        VectorWithMemoryTracking<Float32> best_score(n, std::numeric_limits<Float32>::max());
        for (size_t row = 0; row < n; ++row)
            res[row] = ids.empty() ? 0 : ids[0];

        constexpr size_t tile = 1024; /// tile * dim * 4B stays in L2, reused across all n rows
        VectorWithMemoryTracking<Float32> pack(tile * dim);
        VectorWithMemoryTracking<Float32> acc(tile);
        for (size_t c0 = 0; c0 < k; c0 += tile)
        {
            size_t width = std::min(tile, k - c0);
            for (size_t j = 0; j < dim; ++j) /// pack the tile contiguously (reads ct once per tile)
                std::copy(&ct[j * k + c0], &ct[j * k + c0] + width, &pack[j * width]);

            AssignCentroidImpl::scoreTile(
                vec_data, n, dim, pack.data(), &cnorm[c0], &ids[c0], width,
                acc.data(), best_score.data(), res.data());
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
    bool useDefaultImplementationForConstants() const override { return false; } /// we require arg #1 to stay const
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments: assignCentroid(vec, centroids | dict_name)", name);

        /// The kernel reads the nested column as `ColumnFloat32`, so `Float32` is required exactly - accepting any
        /// float here would reinterpret e.g. `Float64` payload as `Float32` and silently produce wrong ids.
        /// Note that array literals such as `[1.0, 2.0]` are `Array(Float64)` and must be CAST explicitly.
        const auto * vec_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!vec_type || !WhichDataType(vec_type->getNestedType()).isFloat32())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be Array(Float32) (CAST an Array(Float64) or Array(BFloat16) column first)", name);

        if (!isCentroidsArray(arguments[1]) && !isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a constant Array(Array(Float32)) (centroids, CAST an "
                "Array(Array(Float64)) literal first) or a constant String (dictionary name)", name);

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::shared_ptr<const CentroidMatrix> matrix;
        if (isString(arguments[1].type))
        {
            const auto * name_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());
            if (!name_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of {} (dictionary name) must be constant", name);
            matrix = getDictionaryMatrix(name_const->getValue<String>());
        }
        else
        {
            matrix = buildConstMatrix(arguments[1]);
        }

        /// arg #0 may itself be constant (e.g. a literal vector); materialize so the ColumnArray cast is valid.
        ColumnPtr vec_full = arguments[0].column->convertToFullColumnIfConst();
        const auto & vec = assert_cast<const ColumnArray &>(*vec_full);
        const auto & vec_data = assert_cast<const ColumnFloat32 &>(vec.getData()).getData();
        const auto & vec_offsets = vec.getOffsets();

        auto result = ColumnUInt32::create(input_rows_count);
        auto & res = result->getData();
        matrix->assignBlock(vec_data.data(), vec_offsets, input_rows_count, res);
        return result;
    }

private:
    /// Holds the context and performs the `dictGet` access check on first use. Reusing it (instead of keeping a
    /// bare `ContextPtr`) is what the style check asks for, and it also gives us the access check for free.
    mutable FunctionDictHelper dict_helper;
    mutable std::mutex cache_mutex;
    mutable const IDictionary * cached_dict_ptr = nullptr; /// identity changes on dictionary reload
    mutable std::shared_ptr<const CentroidMatrix> cached_matrix;

    static bool isCentroidsArray(const DataTypePtr & type)
    {
        const auto * outer = typeid_cast<const DataTypeArray *>(type.get());
        if (!outer)
            return false;
        const auto * inner = typeid_cast<const DataTypeArray *>(outer->getNestedType().get());
        return inner && WhichDataType(inner->getNestedType()).isFloat32();
    }

    /// Build the matrix from a constant Array(Array(Float32)) argument. Ids are the array positions (0..k-1).
    static std::shared_ptr<const CentroidMatrix> buildConstMatrix(const ColumnWithTypeAndName & arg)
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(arg.column.get());
        if (!col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Centroids argument of assignCentroid must be constant");

        const auto & outer = assert_cast<const ColumnArray &>(col_const->getDataColumn()); /// one row = the k centroids
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

    /// Read the named dictionary once (columns `cid`, `vec`), cache the matrix until the dictionary reloads.
    std::shared_ptr<const CentroidMatrix> getDictionaryMatrix(const String & dict_name) const
    {
        auto dictionary = dict_helper.getDictionary(dict_name);

        {
            std::lock_guard lock(cache_mutex);
            if (cached_matrix && cached_dict_ptr == dictionary.get())
                return cached_matrix;
        }

        /// Full-read the dictionary (same mechanism the `dictionary()` table function uses).
        QueryPipeline pipeline(dictionary->read(Names{"cid", "vec"}, /*max_block_size=*/65536, /*num_streams=*/1));
        PullingPipelineExecutor executor(pipeline);

        VectorWithMemoryTracking<std::pair<UInt64, VectorWithMemoryTracking<Float32>>> centroids;
        Block block;
        while (executor.pull(block))
        {
            const auto & cid_col = block.getByName("cid").column;
            const auto & vec_col = block.getByName("vec");

            /// The dictionary type is only known here (the name is a runtime string), and the kernel below reads
            /// the nested column as `ColumnFloat32`, so reject anything else instead of reinterpreting the payload.
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

        /// Sort by cid so the row order is stable/inspectable (ids are stored explicitly, so order is not required
        /// for correctness).
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
            ids[c] = static_cast<UInt32>(centroids[c].first);
        }

        auto matrix = std::make_shared<CentroidMatrix>();
        matrix->build(row_major.data(), k, dim, ids.data());

        {
            std::lock_guard lock(cache_mutex);
            cached_matrix = matrix;
            cached_dict_ptr = dictionary.get();
        }
        return matrix;
    }
};

}

REGISTER_FUNCTION(AssignCentroid)
{
    FunctionDocumentation::Description description =
        "Returns the id of the nearest (L2) centroid to a vector. The centroids are given as a constant "
        "Array(Array(Float32)) (id = position) or as the name of a Dictionary holding columns (cid, vec) (id = cid).";
    FunctionDocumentation::Syntax syntax = "assignCentroid(vec, centroids | dict_name)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Input vector.", {"Array(Float32)"}},
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
