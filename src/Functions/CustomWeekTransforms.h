#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <base/types.h>
#include <Core/DecimalFunctions.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Common/DateLUTImpl.h>


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
    void vector(const FromVectorType & vec_from, ToVectorType & vec_to, UInt8 week_mode, const DateLUTImpl & time_zone) const
    {
        using ValueType = typename ToVectorType::value_type;
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
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
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/, Transform transform = {})
    {
        const auto op = WeekTransformer<typename FromDataType::FieldType, typename ToDataType::FieldType, Transform, is_extended_result>{std::move(transform)};

        static constexpr UInt8 default_week_mode = 0;
        UInt8 week_mode = default_week_mode;
        if (arguments.size() > 1)
        {
            if (const auto * week_mode_column = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
                week_mode = week_mode_column->getValue<UInt8>();
        }

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        const ColumnPtr source_col = arguments[0].column;
        if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            auto col_to = ToDataType::ColumnType::create();
            op.vector(sources->getData(), col_to->getData(), week_mode, time_zone);
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
