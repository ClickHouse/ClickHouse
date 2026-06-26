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
#include <Common/VectorQuantization.h>
#include <Common/VectorWithMemoryTracking.h>

#include <vector>

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

/// Pull one constant argument as a string (method name). Throws if not constant.
String getConstStringArgument(const ColumnWithTypeAndName & arg, const String & fn, size_t idx)
{
    if (!arg.column || !isColumnConst(*arg.column))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant string", idx + 1, fn);
    return String(arg.column->getDataAt(0));
}

/// Pull one constant argument as an unsigned integer (dimensions / bits / is_l2 flag). Throws if not constant.
UInt64 getConstUIntArgument(const ColumnWithTypeAndName & arg, const String & fn, size_t idx)
{
    if (!arg.column || !isColumnConst(*arg.column))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant integer", idx + 1, fn);
    return arg.column->getUInt(0);
}

/// Read the float vector at `row` from an Array(Float32) / Array(Float64) column into `out` (resized to the array size).
void readVectorRow(const ColumnArray & col_arr, size_t row, VectorWithMemoryTracking<float> & out)
{
    const IColumn & nested = col_arr.getData();
    const auto & offsets = col_arr.getOffsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t size = offsets[row] - begin;
    out.resize(size);

    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&nested))
    {
        const auto & data = f32->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = data[begin + i];
    }
    else if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&nested))
    {
        const auto & data = f64->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = static_cast<float>(data[begin + i]);
    }
    else if (const auto * bf16 = typeid_cast<const ColumnBFloat16 *>(&nested))
    {
        const auto & data = bf16->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = static_cast<float>(data[begin + i]);
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Vector argument must be Array(Float32), Array(Float64) or Array(BFloat16)");
}

void checkVectorArgument(const DataTypePtr & type, const String & fn)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(type.get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of function {} must be Array(Float32), Array(Float64) or Array(BFloat16)", fn);
    const auto & nested = array_type->getNestedType();
    if (!isFloat(nested))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of function {} must be Array(Float32), Array(Float64) or Array(BFloat16), got Array({})", fn, nested->getName());
}

}


/// quantizeEncode(vec, method, dimensions, bits) -> FixedString(bytesPerVector)
///
/// Stateless per-row quantization of `vec` using one of the data-independent methods. The same kernels back the
/// `Quantize` column codec, which stores these codes as a companion stream of the vector column. This scalar function
/// exposes the kernels directly for testing and ad-hoc use. Use `quantizeDistance` to compute an approximate distance
/// from the resulting code at query time.
class FunctionQuantizeEncode : public IFunction
{
public:
    static constexpr auto name = "quantizeEncode";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizeEncode>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkVectorArgument(arguments[0].type, name);

        const String method = getConstStringArgument(arguments[1], name, 1);
        if (!VectorQuantization::isSupportedMethod(method))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown quantization method '{}'", method);

        const UInt64 dimensions = getConstUIntArgument(arguments[2], name, 2);
        const UInt64 bits = getConstUIntArgument(arguments[3], name, 3);
        if (dimensions == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of dimensions must be greater than zero");

        const size_t bytes = VectorQuantization::bytesPerVector(method, dimensions, bits);
        return std::make_shared<DataTypeFixedString>(bytes);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const String method = getConstStringArgument(arguments[1], name, 1);
        const UInt64 dimensions = getConstUIntArgument(arguments[2], name, 2);
        const UInt64 bits = getConstUIntArgument(arguments[3], name, 3);
        const size_t n = assert_cast<const DataTypeFixedString &>(*result_type).getN();

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
                    "Vector at row {} has {} elements but function {} was declared with {} dimensions",
                    row, buf.size(), name, dimensions);

            VectorQuantization::encode(method, buf.data(), dimensions, bits, reinterpret_cast<char *>(&chars[row * n]));
        }

        return col_res;
    }
};


/// quantizeDistance(code, query, method, dimensions, bits, is_l2) -> Float32
///
/// Approximate distance between a quantized `code` (as produced by `quantizeEncode` or the `Quantize` codec) and the
/// full-precision query vector `query`. The query state is prepared once per call. `is_l2` selects L2Distance (1) vs
/// cosineDistance (0).
class FunctionQuantizeDistance : public IFunction
{
public:
    static constexpr auto name = "quantizeDistance";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizeDistance>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 6; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4, 5}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeFixedString>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a FixedString (a quantized code)", name);
        checkVectorArgument(arguments[1].type, name);

        const String method = getConstStringArgument(arguments[2], name, 2);
        if (!VectorQuantization::isSupportedMethod(method))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown quantization method '{}'", method);

        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const String method = getConstStringArgument(arguments[2], name, 2);
        const UInt64 dimensions = getConstUIntArgument(arguments[3], name, 3);
        const UInt64 bits = getConstUIntArgument(arguments[4], name, 4);
        const bool is_l2 = getConstUIntArgument(arguments[5], name, 5) != 0;

        /// Read the (constant) query vector from the constant's single-row payload and prepare the query state once.
        /// Reading the payload directly (rather than expanding the constant) keeps this correct on empty/dry-run blocks,
        /// where the expanded column would have zero rows.
        const auto * query_const = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        if (!query_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Query argument of function {} must be a constant array", name);
        const auto * query_arr = checkAndGetColumn<ColumnArray>(&query_const->getDataColumn());

        VectorWithMemoryTracking<float> query_buf;
        readVectorRow(*query_arr, 0, query_buf);
        if (query_buf.size() != dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Query vector has {} elements but function {} was declared with {} dimensions",
                query_buf.size(), name, dimensions);

        auto query = VectorQuantization::prepareQuery(method, query_buf.data(), dimensions, bits, is_l2);

        const auto * col_code = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());
        if (!col_code)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", name);
        const size_t n = col_code->getN();
        const size_t expected = VectorQuantization::bytesPerVector(method, dimensions, bits);
        if (n != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Quantized code has {} bytes but method '{}' with {} dimensions expects {}", n, method, dimensions, expected);
        const auto & chars = col_code->getChars();

        auto col_res = ColumnFloat32::create(input_rows_count);
        auto & res_data = col_res->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
            res_data[row] = VectorQuantization::distance(*query, reinterpret_cast<const char *>(&chars[row * n]));

        return col_res;
    }
};


REGISTER_FUNCTION(QuantizeEncode)
{
    FunctionDocumentation::Description description = R"(
Encodes a dense vector into a compact, data-independent quantized code for fast brute-force nearest-neighbour search.

Each method is a stateless per-row function of the vector (a fixed-seed random projection or the data-independent E8
lattice; there is no trained codebook). The same kernels back the `Quantize` column codec. Use `quantizeDistance` to
compute an approximate distance from the resulting code at query time.

Supported methods: `rabitq` (1-bit), `turboquant` (2-bit), `e8` (E8 lattice product quantization), and `int8`
(per-coordinate Lloyd-Max scalar quantization). The `bits` argument is only used by `e8` (bits per 8-dimensional
sub-quantizer); it is ignored by the other methods.
)";
    FunctionDocumentation::Syntax syntax = "quantizeEncode(vec, method, dimensions, bits)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "The vector to encode.", {"Array(Float32)", "Array(Float64)", "Array(BFloat16)"}},
        {"method", "Quantization method.", {"const String"}},
        {"dimensions", "Number of vector dimensions.", {"const UInt*"}},
        {"bits", "Bits per sub-quantizer (only used by `e8`).", {"const UInt*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The quantized code.", {"FixedString"}};
    FunctionDocumentation::Examples examples = {
        {"Encode", "SELECT length(quantizeEncode([1., 2., 3., 4., 5., 6., 7., 8.], 'rabitq', 8, 0));", "5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation = {
        description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionQuantizeEncode>(documentation);
}

REGISTER_FUNCTION(QuantizeDistance)
{
    FunctionDocumentation::Description description = R"(
Computes the approximate distance between a quantized code (as produced by `quantizeEncode` or the `Quantize` codec) and
a full-precision query vector. The query state is prepared once per call, so the function is efficient when scanning many
codes against a single query. `is_l2` selects `L2Distance` (1) versus `cosineDistance` (0).
)";
    FunctionDocumentation::Syntax syntax = "quantizeDistance(code, query, method, dimensions, bits, is_l2)";
    FunctionDocumentation::Arguments arguments = {
        {"code", "A quantized code produced by `quantizeEncode`.", {"FixedString"}},
        {"query", "The full-precision query vector.", {"const Array(Float32)", "const Array(Float64)", "const Array(BFloat16)"}},
        {"method", "Quantization method (must match the one used for `code`).", {"const String"}},
        {"dimensions", "Number of vector dimensions.", {"const UInt*"}},
        {"bits", "Bits per sub-quantizer (only used by `e8`).", {"const UInt*"}},
        {"is_l2", "1 for `L2Distance`, 0 for `cosineDistance`.", {"const UInt*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The approximate distance.", {"Float32"}};
    FunctionDocumentation::Examples examples = {
        {"Distance", "SELECT quantizeDistance(quantizeEncode([1., 0.], 'rabitq', 2, 0), [1., 0.], 'rabitq', 2, 0, 1);", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation = {
        description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionQuantizeDistance>(documentation);
}

}
