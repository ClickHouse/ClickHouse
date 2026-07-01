#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/productQuantization.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
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

/// A per-row view of the codebook argument, validated for byte size. `stride` is the number of floats between rows:
/// 0 for a constant codebook (the planner path: `SerializationPQCodebook` reads the per-part codebook as a `ColumnConst`,
/// so row 0 serves every row), or `expected_floats` for a non-constant column (each row uses its own codebook - indexing
/// `row * stride` avoids silently scoring later rows against row 0's codebook).
struct CodebookView
{
    const float * data = nullptr;
    size_t stride = 0;
    const float * row(size_t r) const { return data + r * stride; }
};

CodebookView getCodebook(const ColumnWithTypeAndName & arg, const String & fn, size_t expected_floats)
{
    const ColumnFixedString * cb_fs = nullptr;
    size_t stride = 0;
    if (const auto * cb_const = checkAndGetColumnConst<ColumnFixedString>(arg.column.get()))
        cb_fs = &assert_cast<const ColumnFixedString &>(cb_const->getDataColumn());
    else if ((cb_fs = checkAndGetColumn<ColumnFixedString>(arg.column.get())))
        stride = expected_floats; /// genuine per-row codebook column
    if (!cb_fs || cb_fs->empty())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Codebook argument of function {} must be a (constant) FixedString", fn);
    if (cb_fs->getN() != expected_floats * sizeof(float))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codebook of function {} has {} bytes but the given dimensions/m/nbits expect {}",
            fn, cb_fs->getN(), expected_floats * sizeof(float));
    return {reinterpret_cast<const float *>(cb_fs->getChars().data()), stride};
}

}

/// Internal-only `__pqDistance(code, codebook, query, dimensions, m, nbits, is_l2) -> Float32` (asymmetric ADC).
/// Injected into the query plan by the vector-search optimizer; not registered in `FunctionFactory` (not user-callable).
class FunctionPQDistance : public IFunction
{
public:
    static constexpr auto name = "__pqDistance";

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
        const CodebookView codebook = getCodebook(arguments[1], name, ProductQuantization::codebookFloats(dimensions, m, nbits));

        const auto * query_const = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());
        if (!query_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Query argument of function {} must be a constant array", name);
        const auto * query_arr = checkAndGetColumn<ColumnArray>(&query_const->getDataColumn());
        VectorWithMemoryTracking<float> query_buf;
        readVectorRow(*query_arr, 0, query_buf);
        if (query_buf.size() != dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Query vector has {} elements but function {} was declared with {} dimensions", query_buf.size(), name, dimensions);

        /// Accept a constant code argument (e.g. a literal `FixedString`) by materializing it, so scalar and column use
        /// of the function agree (the codebook argument is handled the same way in getCodebook).
        const ColumnPtr code_column = arguments[0].column->convertToFullColumnIfConst();
        const auto * col_code = checkAndGetColumn<ColumnFixedString>(code_column.get());
        if (!col_code)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", name);
        const size_t n = col_code->getN();
        const size_t expected = ProductQuantization::bytesPerVector(dimensions, m, nbits);
        if (n != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PQ code has {} bytes but m={} nbits={} expect {}", n, m, nbits, expected);
        const auto & chars = col_code->getChars();

        auto col_res = ColumnFloat32::create(input_rows_count);
        auto & res_data = col_res->getData();
        /// Prepare the ADC lookup tables once for a constant codebook (stride 0); rebuild per row for a per-row codebook.
        std::shared_ptr<const ProductQuantization::Query> query;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (!query || codebook.stride != 0)
                query = ProductQuantization::prepareQuery(codebook.row(row), dimensions, m, nbits, query_buf.data(), is_l2);
            res_data[row] = ProductQuantization::distance(*query, reinterpret_cast<const char *>(&chars[row * n]));
        }
        return col_res;
    }
};

FunctionOverloadResolverPtr createInternalFunctionPQDistanceResolver()
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionPQDistance>());
}

}
