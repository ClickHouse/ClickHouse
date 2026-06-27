#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FunctionDocumentation.h>
#include <Common/ProductQuantization.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

UInt64 getConstUInt(const ColumnWithTypeAndName & arg, const String & fn, size_t idx)
{
    if (!arg.column || !isColumnConst(*arg.column))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant integer", idx + 1, fn);
    return arg.column->getUInt(0);
}

/// Read the float vector at `row` of an Array(Float32|Float64|BFloat16) column into `out`.
void readVectorRow(const ColumnArray & col_arr, size_t row, VectorWithMemoryTracking<float> & out)
{
    const IColumn & nested = col_arr.getData();
    const auto & offsets = col_arr.getOffsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t size = offsets[row] - begin;
    out.resize(size);

    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&nested))
        for (size_t i = 0; i < size; ++i) out[i] = f32->getData()[begin + i];
    else if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&nested))
        for (size_t i = 0; i < size; ++i) out[i] = static_cast<float>(f64->getData()[begin + i]);
    else if (const auto * bf16 = typeid_cast<const ColumnBFloat16 *>(&nested))
        for (size_t i = 0; i < size; ++i) out[i] = static_cast<float>(bf16->getData()[begin + i]);
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Vector argument must be Array(Float32|Float64|BFloat16)");
}

void checkVectorArgument(const DataTypePtr & type, const String & fn, size_t idx)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(type.get());
    if (!array_type || !isFloat(array_type->getNestedType()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument #{} of function {} must be Array(Float32|Float64|BFloat16)", idx + 1, fn);
}

/// Reinterpret the codebook FixedString argument as a flat float array; validates the byte size. The codebook is
/// constant within a block: either a query-level constant (`ColumnConst`, ad-hoc use) or the per-part codebook read as
/// a `ColumnConst` by `SerializationPQCodebook` (the planner path). A non-const full column is also accepted (all rows
/// equal), reading row 0.
const float * getCodebook(const ColumnWithTypeAndName & arg, const String & fn, size_t expected_floats)
{
    const ColumnFixedString * cb_fs = nullptr;
    if (const auto * cb_const = checkAndGetColumnConst<ColumnFixedString>(arg.column.get()))
        cb_fs = &assert_cast<const ColumnFixedString &>(cb_const->getDataColumn());
    else
        cb_fs = checkAndGetColumn<ColumnFixedString>(arg.column.get());
    if (!cb_fs || cb_fs->empty())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Codebook argument of function {} must be a (constant) FixedString", fn);
    if (cb_fs->getN() != expected_floats * sizeof(float))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codebook of function {} has {} bytes but the given dimensions/m/nbits expect {}",
            fn, cb_fs->getN(), expected_floats * sizeof(float));
    return reinterpret_cast<const float *>(cb_fs->getChars().data());
}

}

/// pqTrain(samples, dimensions, m, nbits) -> FixedString(k * dimensions * 4)
/// Trains M=`m` per-subspace PQ codebooks (k=2^nbits centroids each) from a sample set of vectors passed as a constant
/// `Array(Array(Float32))` (e.g. `groupArray(vec)` over a sample). Returns the flat codebook as a FixedString.
class FunctionPQTrain : public IFunction
{
public:
    static constexpr auto name = "pqTrain";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPQTrain>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }
    bool useDefaultImplementationForConstants() const override { return false; }
    /// The samples arg is typically an aggregate (groupArray), so it is NOT required constant - only the scalar params.
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * outer = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!outer || !checkAndGetDataType<DataTypeArray>(outer->getNestedType().get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be Array(Array(Float32)) - a set of sample vectors", name);

        const UInt64 dimensions = getConstUInt(arguments[1], name, 1);
        const UInt64 m = getConstUInt(arguments[2], name, 2);
        const UInt64 nbits = getConstUInt(arguments[3], name, 3);
        if (const std::string err = ProductQuantization::validateParams(dimensions, m, nbits); !err.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: {}", name, err);

        return std::make_shared<DataTypeFixedString>(ProductQuantization::codebookFloats(dimensions, m, nbits) * sizeof(float));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const UInt64 dimensions = getConstUInt(arguments[1], name, 1);
        const UInt64 m = getConstUInt(arguments[2], name, 2);
        const UInt64 nbits = getConstUInt(arguments[3], name, 3);
        const size_t fs_bytes = assert_cast<const DataTypeFixedString &>(*result_type).getN();

        /// `samples` is an Array(Array(Float32)) (typically a single-row groupArray, but trained per row in general).
        ColumnPtr samples_col = arguments[0].column->convertToFullColumnIfConst();
        const auto * outer = checkAndGetColumn<ColumnArray>(samples_col.get());
        if (!outer)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be Array(Array(Float32))", name);
        const auto * inner = checkAndGetColumn<ColumnArray>(&outer->getData());
        if (!inner)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be Array(Array(Float32))", name);
        const auto & outer_offsets = outer->getOffsets();

        auto col_res = ColumnFixedString::create(fs_bytes);
        auto & chars = col_res->getChars();
        chars.resize_fill(input_rows_count * fs_bytes, 0);

        VectorWithMemoryTracking<float> flat;
        VectorWithMemoryTracking<float> vbuf;
        for (size_t r = 0; r < input_rows_count; ++r)
        {
            const size_t begin = r == 0 ? 0 : outer_offsets[r - 1];
            const size_t num_samples = outer_offsets[r] - begin;
            flat.resize(num_samples * dimensions);
            for (size_t j = 0; j < num_samples; ++j)
            {
                readVectorRow(*inner, begin + j, vbuf);
                if (vbuf.size() != dimensions)
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "Sample vector #{} has {} elements but function {} was declared with {} dimensions", begin + j, vbuf.size(), name, dimensions);
                std::memcpy(flat.data() + j * dimensions, vbuf.data(), dimensions * sizeof(float));
            }
            std::vector<float> codebook = ProductQuantization::trainCodebook(flat.data(), num_samples, dimensions, m, nbits);
            std::memcpy(&chars[r * fs_bytes], codebook.data(), fs_bytes);
        }
        return col_res;
    }
};

/// pqEncode(vec, codebook, dimensions, m, nbits) -> FixedString(m * code_bytes)
class FunctionPQEncode : public IFunction
{
public:
    static constexpr auto name = "pqEncode";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPQEncode>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 5; }
    bool useDefaultImplementationForConstants() const override { return false; }
    /// The codebook (arg 1) need not be a query constant: it may be the result of `pqTrain` (not constant-folded). Only
    /// the scalar params are required constant; `getCodebook` accepts a const or a uniform full column.
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2, 3, 4}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkVectorArgument(arguments[0].type, name, 0);
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[1].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} (codebook) must be a FixedString", name);
        const UInt64 dimensions = getConstUInt(arguments[2], name, 2);
        const UInt64 m = getConstUInt(arguments[3], name, 3);
        const UInt64 nbits = getConstUInt(arguments[4], name, 4);
        if (const std::string err = ProductQuantization::validateParams(dimensions, m, nbits); !err.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: {}", name, err);
        return std::make_shared<DataTypeFixedString>(ProductQuantization::bytesPerVector(dimensions, m, nbits));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Dry-run / empty blocks pass zero-row (and possibly non-const) columns; nothing to encode, and the codebook is
        /// not materialized yet.
        if (input_rows_count == 0)
            return result_type->createColumn();

        const UInt64 dimensions = getConstUInt(arguments[2], name, 2);
        const UInt64 m = getConstUInt(arguments[3], name, 3);
        const UInt64 nbits = getConstUInt(arguments[4], name, 4);
        const size_t n = assert_cast<const DataTypeFixedString &>(*result_type).getN();
        const float * codebook = getCodebook(arguments[1], name, ProductQuantization::codebookFloats(dimensions, m, nbits));

        ColumnPtr vec_column = arguments[0].column->convertToFullColumnIfConst();
        const auto * col_arr = checkAndGetColumn<ColumnArray>(vec_column.get());
        if (!col_arr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", name);

        auto col_res = ColumnFixedString::create(n);
        auto & chars = col_res->getChars();
        chars.resize_fill(input_rows_count * n, 0);

        VectorWithMemoryTracking<float> buf;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            readVectorRow(*col_arr, row, buf);
            if (buf.size() != dimensions)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Vector at row {} has {} elements but function {} was declared with {} dimensions", row, buf.size(), name, dimensions);
            ProductQuantization::encode(codebook, dimensions, m, nbits, buf.data(), reinterpret_cast<char *>(&chars[row * n]));
        }
        return col_res;
    }
};

/// pqDistance(code, codebook, query, dimensions, m, nbits, is_l2) -> Float32  (asymmetric distance computation)
class FunctionPQDistance : public IFunction
{
public:
    static constexpr auto name = "pqDistance";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPQDistance>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 7; }
    bool useDefaultImplementationForConstants() const override { return false; }
    /// The codebook (arg 1) is NOT a query constant in the planner path - it is the per-part codebook read as a
    /// per-block ColumnConst. Only the scalar params (and the query vector) are required constant.
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2, 3, 4, 5, 6}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a FixedString (a PQ code)", name);
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[1].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} (codebook) must be a FixedString", name);
        checkVectorArgument(arguments[2].type, name, 2);
        const UInt64 dimensions = getConstUInt(arguments[3], name, 3);
        const UInt64 m = getConstUInt(arguments[4], name, 4);
        const UInt64 nbits = getConstUInt(arguments[5], name, 5);
        if (const std::string err = ProductQuantization::validateParams(dimensions, m, nbits); !err.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: {}", name, err);
        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Dry-run / empty blocks pass zero-row (and possibly non-const) columns; nothing to compute, and the codebook
        /// is not materialized yet.
        if (input_rows_count == 0)
            return ColumnFloat32::create();

        const UInt64 dimensions = getConstUInt(arguments[3], name, 3);
        const UInt64 m = getConstUInt(arguments[4], name, 4);
        const UInt64 nbits = getConstUInt(arguments[5], name, 5);
        const bool is_l2 = getConstUInt(arguments[6], name, 6) != 0;
        const float * codebook = getCodebook(arguments[1], name, ProductQuantization::codebookFloats(dimensions, m, nbits));

        const auto * query_const = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());
        if (!query_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Query argument of function {} must be a constant array", name);
        const auto * query_arr = checkAndGetColumn<ColumnArray>(&query_const->getDataColumn());
        VectorWithMemoryTracking<float> query_buf;
        readVectorRow(*query_arr, 0, query_buf);
        if (query_buf.size() != dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Query vector has {} elements but function {} was declared with {} dimensions", query_buf.size(), name, dimensions);

        auto query = ProductQuantization::prepareQuery(codebook, dimensions, m, nbits, query_buf.data(), is_l2);

        const auto * col_code = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());
        if (!col_code)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", name);
        const size_t n = col_code->getN();
        const size_t expected = ProductQuantization::bytesPerVector(dimensions, m, nbits);
        if (n != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PQ code has {} bytes but m={} nbits={} expect {}", n, m, nbits, expected);
        const auto & chars = col_code->getChars();

        auto col_res = ColumnFloat32::create(input_rows_count);
        auto & res_data = col_res->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
            res_data[row] = ProductQuantization::distance(*query, reinterpret_cast<const char *>(&chars[row * n]));
        return col_res;
    }
};

REGISTER_FUNCTION(ProductQuantization)
{
    factory.registerFunction<FunctionPQTrain>(FunctionDocumentation{
        .description = "Trains Product Quantization sub-codebooks (k=2^nbits centroids per subspace) via k-means from a "
                       "constant set of sample vectors `Array(Array(Float32))`. Returns the flat codebook as a FixedString.",
        .syntax = "pqTrain(samples, dimensions, m, nbits)",
        .arguments = {{"samples", "Sample vectors.", {"Array(Array(Float32))"}},
                      {"dimensions", "Vector dimensions.", {"const UInt*"}},
                      {"m", "Number of subspaces (dimensions must be a multiple of m).", {"const UInt*"}},
                      {"nbits", "Bits per subspace code (k = 2^nbits centroids), 1..16.", {"const UInt*"}}},
        .returned_value = {"The trained codebook.", {"FixedString"}},
        .examples = {{"Train", "SELECT length(pqTrain([[1.,2.,3.,4.],[5.,6.,7.,8.]], 4, 2, 1))", "32"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Distance});

    factory.registerFunction<FunctionPQEncode>(FunctionDocumentation{
        .description = "Encodes a vector into Product Quantization codes (m bytes, or 2m for nbits>8) against a trained codebook.",
        .syntax = "pqEncode(vec, codebook, dimensions, m, nbits)",
        .arguments = {{"vec", "Vector to encode.", {"Array(Float32)"}},
                      {"codebook", "Codebook from pqTrain.", {"FixedString"}},
                      {"dimensions", "Vector dimensions.", {"const UInt*"}},
                      {"m", "Number of subspaces.", {"const UInt*"}},
                      {"nbits", "Bits per subspace code.", {"const UInt*"}}},
        .returned_value = {"The PQ code.", {"FixedString"}},
        .examples = {{"Encode", "SELECT length(pqEncode([1.,2.,3.,4.], pqTrain([[1.,2.,3.,4.]], 4, 2, 1), 4, 2, 1))", "2"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Distance});

    factory.registerFunction<FunctionPQDistance>(FunctionDocumentation{
        .description = "Asymmetric distance between a PQ code and a full-precision query vector, using the trained codebook. "
                       "`is_l2` selects L2Distance (1) versus cosineDistance (0).",
        .syntax = "pqDistance(code, codebook, query, dimensions, m, nbits, is_l2)",
        .arguments = {{"code", "PQ code from pqEncode.", {"FixedString"}},
                      {"codebook", "Codebook from pqTrain.", {"FixedString"}},
                      {"query", "Full-precision query vector.", {"const Array(Float32)"}},
                      {"dimensions", "Vector dimensions.", {"const UInt*"}},
                      {"m", "Number of subspaces.", {"const UInt*"}},
                      {"nbits", "Bits per subspace code.", {"const UInt*"}},
                      {"is_l2", "1 for L2Distance, 0 for cosineDistance.", {"const UInt*"}}},
        .returned_value = {"The approximate distance.", {"Float32"}},
        .examples = {{"Distance", "WITH pqTrain([[1.,0.]],2,2,1) AS cb SELECT round(pqDistance(pqEncode([1.,0.], cb, 2,2,1), cb, [1.,0.], 2,2,1, 1), 3)", "0"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Distance});
}

}
