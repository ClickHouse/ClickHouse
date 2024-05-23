#include <numeric>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

class FunctionSeriesKPSS : public IFunction
{
public:
    static constexpr auto name = "seriesKPSS";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesKPSS>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"series", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"}};

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeNumber<Float64>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr col = arguments[0].column;
        const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(col.get());

        const IColumn & arr_data = col_arr->getData();
        const ColumnArray::Offsets & arr_offsets = col_arr->getOffsets();

        Float64 result;

        auto res = ColumnFloat64::create(input_rows_count);
        auto & res_data = res->getData();

        ColumnArray::Offset prev_src_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnArray::Offset curr_offset = arr_offsets[i];
            if (executeNumber<UInt8>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<UInt16>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<UInt32>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<UInt64>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Int8>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Int16>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Int32>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Int64>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Float32>(arr_data, result, prev_src_offset, curr_offset)
                || executeNumber<Float64>(arr_data, result, prev_src_offset, curr_offset))
            {
                res_data[i] = result;
                prev_src_offset = curr_offset;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    getName());
        }
        return res;
    }

private:
    template <typename T>
    bool executeNumber(const IColumn & src_data, Float64 & result, ColumnArray::Offset & start, ColumnArray::Offset & end) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        chassert(start <= end);

        size_t n = end - start;
        Float64 mean = std::accumulate(src_vec.begin() + start, src_vec.begin() + end, 0.0) / n;

        std::vector<Float64> deviations(n);
        std::transform(src_vec.begin() + start, src_vec.begin() + end, deviations.begin(), [mean](double x) { return x - mean; });

        std::vector<double> cum_sum(n);
        std::partial_sum(deviations.begin(), deviations.end(), cum_sum.begin());

        Float64 s2 = std::inner_product(deviations.begin(), deviations.end(), deviations.begin(), 0.0) / n;

        Float64 kpss_statistic = std::inner_product(cum_sum.begin(), cum_sum.end(), cum_sum.begin(), 0.0);

        result = kpss_statistic / (n * n * s2);
        return true;
    }
};

REGISTER_FUNCTION(SeriesKPSS)
{
    factory.registerFunction<FunctionSeriesKPSS>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
}
}
