#pragma once

#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>

#include <common/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DECIMAL_OVERFLOW;
}

/** Casts DateTim64 to or from Int64 representation narrowed down (or scaled up) to any scale value defined in Impl.
 */
template <typename Impl>
class FunctionUnixTimestamp64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static constexpr auto target_scale = Impl::target_scale;

    using SourceDataType = typename Impl::SourceDataType;
    using ResultDataType = typename Impl::ResultDataType;

    static constexpr bool is_result_datetime64 = std::is_same_v<ResultDataType, DataTypeDateTime64>;

    static_assert(std::is_same_v<SourceDataType, DataTypeDateTime64> || std::is_same_v<ResultDataType, DataTypeDateTime64>);

    static auto create(const Context &)
    {
        return std::make_shared<FunctionUnixTimestamp64<Impl>>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return is_result_datetime64 ? 2 : 1; }
    bool isVariadic() const override { return is_result_datetime64; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (is_result_datetime64)
        {
            validateFunctionArgumentTypes(*this, arguments,
                    FunctionArgumentDescriptors{{"value", isDataType<SourceDataType>, nullptr, std::string(SourceDataType::family_name).c_str()}},
                    // optional
                    FunctionArgumentDescriptors{
    //                    {"precision", isDataType<DataTypeUInt8>, isColumnConst, ("Precision of the result, default is " + std::to_string(target_scale)).c_str()},
                        {"timezone", isStringOrFixedString, isColumnConst, "Timezone of the result"},
                    });
            const auto timezone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
            return std::make_shared<DataTypeDateTime64>(target_scale, timezone);
        }
        else
        {
            validateFunctionArgumentTypes(*this, arguments,
                    FunctionArgumentDescriptors{{"value", isDataType<SourceDataType>, nullptr, std::string(SourceDataType::family_name).c_str()}});
            return std::make_shared<DataTypeInt64>();
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        using SourceColumnType = typename SourceDataType::ColumnType;
        using ResultColumnType = typename ResultDataType::ColumnType;

        const auto & src = block.getByPosition(arguments[0]);
        auto & res = block.getByPosition(result);
        const auto & col = *src.column;

        const SourceColumnType * source_col_typed = checkAndGetColumn<SourceColumnType>(col);
        if (!source_col_typed && !(source_col_typed = checkAndGetColumnConstData<SourceColumnType>(&col)))
            throw Exception("Invalid column type" + col.getName() + " expected "
                    + std::string(SourceDataType::family_name),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        res.column = res.type->createColumn();

        if (input_rows_count == 0)
            return;

        auto & result_data = assert_cast<ResultColumnType &>(res.column->assumeMutableRef()).getData();
        result_data.reserve(source_col_typed->size());
        const auto & source_data = source_col_typed->getData();

        const auto scale_diff = getScaleDiff(*checkAndGetDataType<SourceDataType>(src.type.get()), *checkAndGetDataType<ResultDataType>(res.type.get()));
        if (scale_diff == 0)
        {
            static_assert(sizeof(typename SourceColumnType::Container::value_type) == sizeof(typename ResultColumnType::Container::value_type));
            // no conversion necessary
            result_data.push_back_raw_many(source_data.size(), source_data.data());
        }
        else if (scale_diff < 0)
        {
            const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(std::abs(scale_diff));
            for (const auto & v : source_data)
            {
                Int64 result_value = toDestValue(v);
                if (common::mulOverflow(result_value, scale_multiplier, result_value))
                    throw Exception("Decimal overflow in " + getName(), ErrorCodes::DECIMAL_OVERFLOW);

                result_data.push_back(result_value);
            }
        }
        else
        {
            const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(scale_diff);
            for (const auto & v : source_data)
                result_data.push_back(static_cast<Int64>(toDestValue(v) / scale_multiplier));
        }
    }

private:
    static Int64 getScaleDiff(const SourceDataType & src, const ResultDataType & dst)
    {
        Int64 src_scale = target_scale;
        if constexpr (std::is_same_v<SourceDataType, DataTypeDateTime64>)
        {
            src_scale = src.getScale();
        }

        Int64 dst_scale = target_scale;
        if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            dst_scale = dst.getScale();
        }

        return src_scale - dst_scale;
    }

    static auto toDestValue(const DateTime64 & v)
    {
        return Int64{v.value};
    }

    template <typename T>
    static auto toDestValue(const T & v)
    {
        return Int64{v};
    }
};

}
