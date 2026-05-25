#include "config.h"

#if USE_H3

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// `stringToH3` accepts the same inputs as the original C H3 library, which used
/// `sscanf("%" PRIx64, ...)` to parse the index. That means parsing is permissive:
/// the longest valid hex prefix is consumed (after optional whitespace, sign, and
/// "0x"/"0X" prefix), and trailing garbage is ignored, so non-H3 strings like
/// `"foo"` parse to a valid value (e.g. `0xf`) rather than failing. The returned
/// `H3Index` is not validated to be a real H3 cell — use `h3IsValid` for that.
///
/// A returned value of `0` is ambiguous: it can come either from a successful
/// parse of a zero value (e.g. `"0"`, `"000"`, `"0x"`, `"+0x"`) or from the
/// no-hex-digit path where the parse error from the underlying library is
/// swallowed and the default-initialized index is returned (the v4 H3 C API
/// reports errors through a return code, v3 returned `0` silently). Callers
/// must not treat `0` as a definite invalid-input signal.

namespace
{

using namespace GatherUtils;

class FunctionStringToH3 final : public IFunction
{
public:
    static constexpr auto name = "stringToH3";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringToH3>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isStringOrFixedString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument {} of function {}. "
                "Must be String or FixedString", arg->getName(), std::to_string(1), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        if (const auto * h3index = checkAndGetColumn<ColumnString>(col_hindex))
            execute<StringSource>(StringSource(*h3index), dst_data);
        else if (const auto * h3index_fixed = checkAndGetColumn<ColumnFixedString>(col_hindex))
            execute<FixedStringSource>(FixedStringSource(*h3index_fixed), dst_data);
        else if (const ColumnConst * h3index_const = checkAndGetColumnConst<ColumnString>(col_hindex))
            execute<ConstSource<StringSource>>(ConstSource<StringSource>(*h3index_const), dst_data);
        else if (const ColumnConst * h3index_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_hindex))
            execute<ConstSource<FixedStringSource>>(ConstSource<FixedStringSource>(*h3index_const_fixed), dst_data);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column as argument of function {}", getName());

        return dst;
    }

private:
    template <typename H3IndexSource>
    static void execute(H3IndexSource h3index_source, PaddedPODArray<UInt64> & res_data)
    {
        size_t row_num = 0;

        while (!h3index_source.isEnd())
        {
            auto h3index = h3index_source.getWhole();

            // convert to std::string and get the c_str to have the delimiting \0 at the end.
            auto h3index_str = std::string(reinterpret_cast<const char *>(h3index.data), h3index.size);
            H3Index h3_index = 0;
            (void)stringToH3(h3index_str.data(), &h3_index);
            res_data[row_num] = h3_index;

            h3index_source.next();
            ++row_num;
        }
    }
};

}

REGISTER_FUNCTION(StringToH3)
{
    FunctionDocumentation::Description description = R"(
Converts the string representation of an H3 index to the `H3Index` ([UInt64](/sql-reference/data-types/int-uint)) representation.

Parsing follows `sscanf("%" PRIx64, ...)` semantics: the longest valid hex prefix is consumed (after optional whitespace, sign, and `0x`/`0X` prefix) and trailing garbage is ignored, so inputs that contain at least one hex digit succeed. The returned value is not validated to be a real H3 cell — use `h3IsValid` to check that.
    )";
    FunctionDocumentation::Syntax syntax = "stringToH3(index_str)";
    FunctionDocumentation::Arguments arguments = {
        {"index_str", "String representation of the H3 index.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the parsed `H3Index`. A returned value of `0` is ambiguous: it can come either from a successful parse of a zero value (e.g. `'0'`, `'000'`, `'0x'`, `'+0x'`) or from the no-hex-digit path where the parse error is swallowed. The longest valid hex prefix is parsed (e.g. `'foo'` parses to `0xf`). The result is not validated as a real H3 cell.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Convert string to H3 index",
            "SELECT stringToH3('89184926cc3ffff') AS index",
            R"(
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionStringToH3>(documentation);
}

}

#endif
