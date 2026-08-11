#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace Setting
{
    extern const SettingsBool array_count_legacy_uint32_result;
}

/** arrayCount(x1,...,xn -> expression, array1,...,arrayn) - for how many elements of the array the expression is true.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
template <typename ResultType>
struct ArrayCountImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        /// UInt64, and not UInt32: an array can contain more than 2^32 elements, and the count of the
        /// matching ones has to be exact for such an array as well. UInt32 is kept only behind the
        /// `array_count_legacy_uint32_result` compatibility setting.
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result is expected to be a UInt8", mapped->getDataType());

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnVector<ResultType>::create(offsets.size());
                typename ColumnVector<ResultType>::Container & out_counts = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_counts[i] = static_cast<ResultType>(offsets[i] - pos);
                    pos = offsets[i];
                }

                return out_column;
            }
            return DataTypeNumber<ResultType>().createColumnConst(array.size(), ResultType(0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnVector<ResultType>::create(offsets.size());
        typename ColumnVector<ResultType>::Container & out_counts = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            ResultType count = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                    ++count;
            }
            out_counts[i] = count;
        }

        return out_column;
    }
};

struct NameArrayCount { static constexpr auto name = "arrayCount"; };

/// Chooses the result type by the `array_count_legacy_uint32_result` compatibility setting:
/// `UInt64` (exact for arrays of any size) by default, `UInt32` as before version 26.8.
struct ArrayCountFunctionChooser
{
    static constexpr auto name = NameArrayCount::name;

    static FunctionPtr create(ContextPtr context)
    {
        if (context->getSettingsRef()[Setting::array_count_legacy_uint32_result])
            return std::make_shared<FunctionArrayMapped<ArrayCountImpl<UInt32>, NameArrayCount>>();
        return std::make_shared<FunctionArrayMapped<ArrayCountImpl<UInt64>, NameArrayCount>>();
    }
};

REGISTER_FUNCTION(ArrayCount)
{
    FunctionDocumentation::Description description = R"(
Returns the number of elements for which `func(arr1[i], ..., arrN[i])` returns true.
If `func` is not specified, it returns the number of non-zero elements in the array.

`arrayCount` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).

:::note Use setting `array_count_legacy_uint32_result` to return `UInt32`
Version 26.8 introduced a backward-incompatible change: `arrayCount` returns `UInt64` instead of `UInt32`, so that the result is exact for arrays with more than `4294967295` matching elements.
To retain the previous behavior, set setting `array_count_legacy_uint32_result` (default: `false`) to `true`.

During a rolling upgrade of a cluster, a distributed query initiated by a not-yet-upgraded server does not forward this setting, so type-sensitive expressions evaluated locally on already-upgraded shards (for example, `byteSize(arrayCount(...))`) observe `UInt64` there. To keep such queries fully unchanged until the whole cluster is upgraded, set `array_count_legacy_uint32_result = 1` on the upgraded servers for the users under which shard-side queries execute, and remove it after the upgrade is complete. Which user that is depends on the cluster configuration: with an interserver `secret` configured, the shard runs the query as the initiator's current user; otherwise it is the user from the cluster definition or from the `remote` table function (`default` unless specified). The simplest robust approach is to enable the setting for all users of the upgraded servers.
:::
    )";
    FunctionDocumentation::Syntax syntax = "arrayCount([func, ] arr1, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"func", "Optional. Function to apply to each element of the array(s).", {"Lambda function"}},
        {"arr1, ..., arrN", "N arrays.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of elements for which `func` returns true. Otherwise, returns the number of non-zero elements in the array.", {"UInt64"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT arrayCount(x -> (x % 2), groupArray(number)) FROM numbers(10)", "5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    factory.registerFunction<ArrayCountFunctionChooser>(documentation);
}

}


