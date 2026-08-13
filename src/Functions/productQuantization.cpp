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
#include <Common/ProductQuantizer.h>
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

/// Read the float vector at `row` of an Array(Float32|Float64|BFloat16) column into `result`.
void readVectorRow(const ColumnArray & col_arr, size_t row, VectorWithMemoryTracking<float> & result)
{
    const IColumn & array_data = col_arr.getData();
    const auto & array_offsets = col_arr.getOffsets();
    const size_t begin = row == 0 ? 0 : array_offsets[row - 1];
    const size_t size = array_offsets[row] - begin;
    result.resize(size);

    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&array_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = f32->getData()[begin + i];
    }
    else if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&array_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = static_cast<float>(f64->getData()[begin + i]);
    }
    else if (const auto * bf16 = typeid_cast<const ColumnBFloat16 *>(&array_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = static_cast<float>(bf16->getData()[begin + i]);
    }
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

/// A per-row view of the codebook argument, validated for byte size.
/// `stride` is the number of floats between rows, it's 0 for a constant codebook.
struct CodebookView
{
    const float * data = nullptr;
    size_t stride = 0;
    const float * row(size_t r) const { return data + r * stride; }
};

CodebookView getCodebook(const ColumnWithTypeAndName & arg, const String & fn, size_t expected_floats)
{
    const ColumnFixedString * col_codebook = nullptr;
    size_t stride = 0;
    if (const auto * cb_const = checkAndGetColumnConst<ColumnFixedString>(arg.column.get()))
        col_codebook = &assert_cast<const ColumnFixedString &>(cb_const->getDataColumn()); /// const codebook
    else if ((col_codebook = checkAndGetColumn<ColumnFixedString>(arg.column.get())))
        stride = expected_floats; /// per-row codebook
    if (!col_codebook || col_codebook->empty())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Codebook argument of function {} must be a (constant) FixedString", fn);
    if (col_codebook->getN() != expected_floats * sizeof(float))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codebook of function {} has {} bytes but the given dimensions/m/nbits expect {}",
            fn, col_codebook->getN(), expected_floats * sizeof(float));
    return {reinterpret_cast<const float *>(col_codebook->getChars().data()), stride};
}

}

/// Internal function `__productQuantizationDistance(quantized_vector, codebook, vector, dimensions, m, nbits, is_l2) -> Float32`.
///
/// Injected into the query plan by the vector-search optimizer; not registered in `FunctionFactory` (not user-callable).
class FunctionPQDistance : public IFunction
{
public:
    static constexpr auto name = "__productQuantizationDistance";

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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} (a PQ code) must be a FixedString", name);
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[1].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} (codebook) must be a FixedString", name);
        checkVectorArgument(arguments[2].type, name, 2);
        const UInt64 dimensions = getConstUInt(arguments[3], name, 3);
        const UInt64 m = getConstUInt(arguments[4], name, 4);
        const UInt64 nbits = getConstUInt(arguments[5], name, 5);
        if (const std::string err = ProductQuantizer::validateParams(dimensions, m, nbits); !err.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: {}", name, err);
        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnFloat32::create();

        const UInt64 dimensions = getConstUInt(arguments[3], name, 3);
        const UInt64 m = getConstUInt(arguments[4], name, 4);
        const UInt64 nbits = getConstUInt(arguments[5], name, 5);
        const bool is_l2 = getConstUInt(arguments[6], name, 6) != 0;
        const CodebookView codebook = getCodebook(arguments[1], name, ProductQuantizer::codebookFloats(dimensions, m, nbits));

        const auto * col_vector_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());
        if (!col_vector_const_arr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Query argument of function {} must be a constant array", name);
        const auto * col_vector_arr = checkAndGetColumn<ColumnArray>(&col_vector_const_arr->getDataColumn());
        VectorWithMemoryTracking<float> vectors;
        readVectorRow(*col_vector_arr, 0, vectors);
        if (vectors.size() != dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Query vector has {} elements but function {} was declared with {} dimensions", vectors.size(), name, dimensions);

        /// Accept a constant code argument (e.g. a literal `FixedString`) by materializing it, so scalar and column use
        /// of the function agree (the codebook argument is handled the same way in getCodebook).
        const ColumnPtr col_code_full = arguments[0].column->convertToFullColumnIfConst();
        const auto * col_code = checkAndGetColumn<ColumnFixedString>(col_code_full.get());
        if (!col_code)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", name);
        const size_t n = col_code->getN();
        const size_t expected = ProductQuantizer::bytesPerVector(dimensions, m, nbits);
        if (n != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PQ code has {} bytes but m={} nbits={} expect {}", n, m, nbits, expected);
        const auto & chars = col_code->getChars();

        auto col_res = ColumnFloat32::create(input_rows_count);
        auto & res_data = col_res->getData();
        /// Prepare the ADC lookup tables once for a constant codebook (stride 0); rebuild per row for a per-row codebook.
        ProductQuantizer::QueryPtr query;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (!query || codebook.stride != 0)
                query = ProductQuantizer::prepareQuery(codebook.row(row), dimensions, m, nbits, vectors.data(), is_l2);
            res_data[row] = ProductQuantizer::distance(*query, reinterpret_cast<const char *>(&chars[row * n]));
        }
        return col_res;
    }
};

FunctionOverloadResolverPtr createInternalFunctionPQDistanceResolver()
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionPQDistance>());
}

}
