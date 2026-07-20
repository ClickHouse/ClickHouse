#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <base/types.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


template <typename FromType, typename ToType, typename Transform, bool is_extended_result = false>
struct WeekTransformer
{
    explicit WeekTransformer(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromVectorType, typename ToVectorType>
    void vector(const FromVectorType & vec_from, ToVectorType & vec_to, UInt8 week_mode, const DateLUTImpl & time_zone, size_t input_rows_count) const
    {
        using ValueType = typename ToVectorType::value_type;
        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if constexpr (is_extended_result)
                vec_to[i] = static_cast<ValueType>(transform.executeExtendedResult(vec_from[i], week_mode, time_zone));
            else
                vec_to[i] = static_cast<ValueType>(transform.execute(vec_from[i], week_mode, time_zone));
        }
    }

private:
    const Transform transform;
};


template <typename FromDataType, typename ToDataType, bool is_extended_result = false>
struct CustomWeekTransformImpl
{
    template <typename Transform>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count, Transform transform = {})
    {
        const auto op = WeekTransformer<typename FromDataType::FieldType, typename ToDataType::FieldType, Transform, is_extended_result>{transform};

        static constexpr UInt8 default_week_mode = 0;
        UInt8 week_mode = default_week_mode;
        if (arguments.size() > 1)
        {
            if (const auto * week_mode_column = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
                week_mode = week_mode_column->getValue<UInt8>();
        }

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        const ColumnPtr source_col = arguments[0].column;

        if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
            const auto * sources = checkAndGetColumn<DataTypeString::ColumnType>(source_col.get());

            auto col_to = ToDataType::ColumnType::create();
            col_to->getData().resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                DateTime64 dt64;
                ReadBufferFromString buf(sources->getDataAt(i).toView());
                parseDateTime64BestEffort(dt64, 0, buf, time_zone, utc_time_zone);
                col_to->getData()[i] = static_cast<ToDataType::FieldType>(transform.execute(dt64, week_mode, time_zone));
            }

            return col_to;
        }
        else if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            auto col_to = ToDataType::ColumnType::create();
            op.vector(sources->getData(), col_to->getData(), week_mode, time_zone, input_rows_count);
            return col_to;
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), Transform::name);
        }
    }
};

}
