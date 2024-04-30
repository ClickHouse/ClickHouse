#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>
#include "DataTypes/DataTypeTuple.h"
#include "base/types.h"
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Returns two last coeffitients of Holt-Winters forecast method
class FunctionHoltForecast : public IFunction
{
public:
    static constexpr auto name = "HoltForecast";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHoltForecast>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"time_series", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"alpha", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), isColumnConst, "Number"},
            {"betta", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), isColumnConst, "Number"},};

        validateFunctionArgumentTypes(*this, arguments, args);

        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>()
        };
        return std::make_shared<DataTypeTuple>(std::move(types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr col = arguments[0].column;
        const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(col.get());

        const IColumn & arr_data = col_arr->getData();
        const ColumnArray::Offsets & arr_offsets = col_arr->getOffsets();


        Float64 alpha = arguments[1].column->getFloat64(0);
        Float64 betta = arguments[2].column->getFloat64(0);

        if (isnan(alpha) || !isFinite(alpha) || alpha < 0.0 || alpha > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second argument of function {} must be in range [0, 1]", getName());

        
        if (isnan(betta) || !isFinite(betta) || betta < 0.0 || betta > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The third argument of function {} must be in range [0, 1]", getName());
        
        Float64 level, trend;

        auto levels = ColumnFloat64::create(input_rows_count);
        auto trends = ColumnFloat64::create(input_rows_count);

        auto & levels_data = levels->getData();
        auto & trends_data = trends->getData();


        ColumnArray::Offset prev_src_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnArray::Offset curr_offset = arr_offsets[i];
            if (calculateHolt<UInt8>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<UInt16>(arr_data, alpha, betta,  level, trend,prev_src_offset, curr_offset)
                || calculateHolt<UInt32>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<UInt64>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Int8>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Int16>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Int32>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Int64>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Float32>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset)
                || calculateHolt<Float64>(arr_data, alpha, betta, level, trend, prev_src_offset, curr_offset))
            {
                levels_data[i] = level;
                trends_data[i] = trend;
                prev_src_offset = curr_offset;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    getName());
            }
        MutableColumns columns;
        columns.emplace_back(std::move(levels));
        columns.emplace_back(std::move(trends));
        return ColumnTuple::create(std::move(columns));
    }

private:
    template <typename T>
    bool calculateHolt(
        const IColumn & src_data,
        Float64 alpha,
        Float64 betta,
        Float64 & level,
        Float64 & trend,
        ColumnArray::Offset & start, ColumnArray::Offset & end) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        chassert(start <= end);

        std::vector<Float64> src((src_vec.begin() + start), (src_vec.begin() + end));
        bool first = true;
        for (auto value: src) 
        {
            if (first)
            {
                level = value;
                trend = value;
                first = false;
                continue;
            }
            Float64 new_level = alpha * value + (1.0 - alpha) * (level + trend);
            trend = betta * (new_level - level) + (1.0 - betta) * trend;
            level = new_level;
        }

        return true;
    }
};

REGISTER_FUNCTION(HoltForecast)
{
    factory.registerFunction<FunctionHoltForecast>(FunctionDocumentation{
        .description = R"(
Holts method for Time Series analysus.
Usage: HoltForecast(time_series, alpha, betta) -> (l_n, b_n).
```)",
        .categories{"Time series analysis"}});
}
}
