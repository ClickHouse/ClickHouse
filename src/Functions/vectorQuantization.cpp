#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/vectorQuantization.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/VectorQuantizer.h>
#include <Common/VectorWithMemoryTracking.h>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

String getConstStringArgument(const ColumnWithTypeAndName & arg, const String & fn, size_t idx)
{
    if (!arg.column || !isColumnConst(*arg.column))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant string", idx + 1, fn);
    return String(arg.column->getDataAt(0));
}

UInt64 getConstUIntArgument(const ColumnWithTypeAndName & arg, const String & fn, size_t idx)
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

}


/// Internal function `__quantizeDistance(quantized_vector, vector, method, dimensions, bits, is_l2) -> Float32`.
///
/// Calculates the approximate distance between a data-independent `Quantize(...)` codec's `quantized_vector` and the
/// full-precision query vector `vector`.
/// The query state is prepared once per call.
/// `is_l2` selects L2Distance (1) vs cosineDistance (0).
/// Injected into the query plan by the vector-search optimizer; not registered in `FunctionFactory` (not user-callable).
class FunctionQuantizeDistance : public IFunction
{
public:
    static constexpr auto name = "__quantizeDistance";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 6; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4, 5}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a FixedString (a quantized vector)", name);
        checkVectorArgument(arguments[1].type, name, 1);

        const String method = getConstStringArgument(arguments[2], name, 2);
        const UInt64 dimensions = getConstUIntArgument(arguments[3], name, 3);
        const UInt64 bits = getConstUIntArgument(arguments[4], name, 4);
        if (const auto err = VectorQuantizer::validateParams(method, dimensions, bits); !err.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: {}", name, err);

        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnFloat32::create();

        const String method = getConstStringArgument(arguments[2], name, 2);
        const UInt64 dimensions = getConstUIntArgument(arguments[3], name, 3);
        const UInt64 bits = getConstUIntArgument(arguments[4], name, 4);
        const bool is_l2 = getConstUIntArgument(arguments[5], name, 5) != 0;

        const auto * col_vector_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        if (!col_vector_const_arr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Query argument of function {} must be a constant array", name);
        const auto * col_vector_arr = checkAndGetColumn<ColumnArray>(&col_vector_const_arr->getDataColumn());

        VectorWithMemoryTracking<float> vectors;
        readVectorRow(*col_vector_arr, 0, vectors);
        if (vectors.size() != dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Query vector has {} elements but function {} was declared with {} dimensions",
                vectors.size(), name, dimensions);

        auto query = VectorQuantizer::prepareQuery(method, vectors.data(), dimensions, bits, is_l2);

        const auto * col_code = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());
        if (!col_code)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", name);
        const size_t n = col_code->getN();
        const size_t expected = VectorQuantizer::bytesPerVector(method, dimensions, bits);
        if (n != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Quantized vector has {} bytes but method '{}' with {} dimensions expects {}", n, method, dimensions, expected);
        const auto & code_char = col_code->getChars();

        auto col_res = ColumnFloat32::create(input_rows_count);
        auto & res_data = col_res->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
            res_data[row] = VectorQuantizer::distance(*query, reinterpret_cast<const char *>(&code_char[row * n]));

        return col_res;
    }
};


FunctionOverloadResolverPtr createInternalFunctionQuantizeDistanceResolver()
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionQuantizeDistance>());
}

}
