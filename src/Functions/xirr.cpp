#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/IColumn.h>
#include "Common/Exception.h"
#include <Common/FunctionDocumentation.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <boost/math/tools/roots.hpp>

#include <algorithm>
#include <expected>
#include <limits>
#include <span>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct XirrCalculator
{
    static constexpr int MAX_ITERATIONS = 100;
    static constexpr double START_LOWER_BOUND = -0.999999;
    static constexpr double START_UPPER_BOUND = 100.0;

    enum class XirrErrorCode
    {
        CANNOT_EVALUATE_NPV,
        CANNOT_CONVERGE_DUE_TO_ROUNDING_ERRORS,
        CANNOT_CONVERGE_DUE_TO_INVALID_ARGUMENTS,
        CANNOT_CONVERGE_TOO_MANY_ITERATIONS,
        INPUT_DATES_NOT_SORTED_UNIQUE,
        OTHER_ERROR
    };

    template <typename T, typename D>
    static std::expected<double, XirrErrorCode> calculateXirr(std::span<T> cashflows, std::span<D> & dates)
    {
        if (cashflows.size() != dates.size()) [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size");

        if (cashflows.size() <= 1) [[unlikely]]
            return std::numeric_limits<double>::quiet_NaN();

        if (std::all_of(cashflows.begin(), cashflows.end(), [](T cf) { return cf == 0; })) [[unlikely]]
            return std::numeric_limits<double>::quiet_NaN();

        for (size_t i = 1; i < dates.size(); ++i)
        {
            if (dates[i] <= dates[i - 1]) [[unlikely]]
                return std::unexpected(XirrErrorCode::INPUT_DATES_NOT_SORTED_UNIQUE);
        }

        auto npv_function = [&](double rate)
        {
            if (rate <= -1.0)
                return std::numeric_limits<double>::infinity();

            double npv = 0.0;

            for (size_t i = 0; i < cashflows.size(); ++i)
            {
                double time = (dates[i] - dates[0]) / 365.0;
                if (time == 0.0)
                    npv += cashflows[i];
                else
                    npv += cashflows[i] / std::pow(1.0 + rate, time);
            }

            return npv;
        };

        auto npv_derivative = [&](double rate)
        {
            if (rate <= -1.0)
                return std::numeric_limits<double>::infinity();

            double derivative = 0.0;

            for (size_t i = 0; i < cashflows.size(); ++i)
            {
                double time = (dates[i] - dates[0]) / 365.0;
                if (time != 0.0)
                    derivative -= cashflows[i] * time / std::pow(1.0 + rate, time + 1);
            }

            return derivative;
        };

        try
        {
            boost::uintmax_t max_iter = MAX_ITERATIONS;

            double result = boost::math::tools::newton_raphson_iterate(
                [&](double x) { return std::make_tuple(npv_function(x), npv_derivative(x)); },
                0.1,
                START_LOWER_BOUND,
                START_UPPER_BOUND,
                std::numeric_limits<double>::digits - 4,
                max_iter);

            if (!std::isnan(result) && result > START_LOWER_BOUND && result < START_UPPER_BOUND)
                return result;

            max_iter = MAX_ITERATIONS;
            boost::math::tools::eps_tolerance<double> tol(std::numeric_limits<double>::digits - 4);
            auto toms_result = boost::math::tools::toms748_solve(npv_function, START_LOWER_BOUND, START_UPPER_BOUND, tol, max_iter);

            return (toms_result.first + toms_result.second) / 2;
        }
        catch (const boost::math::evaluation_error &)
        {
            return std::unexpected(XirrErrorCode::CANNOT_EVALUATE_NPV);
        }
        catch (const boost::math::rounding_error &)
        {
            return std::unexpected(XirrErrorCode::CANNOT_CONVERGE_DUE_TO_ROUNDING_ERRORS);
        }
        catch (const std::domain_error &)
        {
            return std::unexpected(XirrErrorCode::CANNOT_CONVERGE_DUE_TO_INVALID_ARGUMENTS);
        }
        catch (...)
        {
            return std::unexpected(XirrErrorCode::OTHER_ERROR);
        }
    }
};


bool isCashFlowColumn(const IDataType & type)
{
    return isArray(type) && isNativeNumber(checkAndGetDataType<DataTypeArray>(type).getNestedType());
}

bool isXirrDateColumn(const IDataType & type)
{
    return isArray(type) && isDateOrDate32(checkAndGetDataType<DataTypeArray>(type).getNestedType());
}

class FunctionXirr : public IFunction
{
public:
    static constexpr auto name = "xirr";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionXirr>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"cashflow", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn), nullptr, "Array[Number]"},
            {"date", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isXirrDateColumn), nullptr, "Array[NativeNumber]"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cashflow_col = arguments[0].column->convertToFullColumnIfConst();
        auto date_col = arguments[1].column->convertToFullColumnIfConst();

        const auto * cashflow_array = checkAndGetColumn<ColumnArray>(cashflow_col.get());
        const auto * date_array = checkAndGetColumn<ColumnArray>(date_col.get());

        if (!cashflow_array || !date_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Both cashflow and date arguments must be arrays");

        auto result_col = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = result_col->getData();

        const ColumnArray::Offsets & cashflow_offsets = cashflow_array->getOffsets();
        const ColumnArray::Offsets & date_offsets = date_array->getOffsets();
        if (cashflow_offsets.size() != date_offsets.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same number of rows");

        const auto * cashflow_data = &cashflow_array->getData();
        const auto * date_data = &date_array->getData();

        auto process_arrays = [&](const auto * cashflow_values, const auto * date_values)
        {
            ColumnArray::Offset previous_offset = 0;
            for (size_t i = 0; i < cashflow_offsets.size(); ++i)
            {
                const auto current_offset = cashflow_offsets[i];
                if (current_offset != date_offsets[i])
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size for each row");

                const auto length = current_offset - previous_offset;
                auto cashflow_span = std::span(cashflow_values->getData().data() + previous_offset, length);
                auto date_span = std::span(date_values->getData().data() + previous_offset, length);

                auto xirr = XirrCalculator::calculateXirr(cashflow_span, date_span);
                if (xirr.has_value()) [[likely]]
                    result_data[i] = xirr.value();
                else
                    result_data[i] = std::numeric_limits<double>::quiet_NaN();

                previous_offset = current_offset;
            }
        };
        auto dispatch_date = [&](const auto * typed_cashflow_data, const IColumn * generic_date_data)
        {
            if (const auto * d = typeid_cast<const ColumnDate *>(generic_date_data))
                process_arrays(typed_cashflow_data, d);
            else if (const auto * d32 = typeid_cast<const ColumnDate32 *>(generic_date_data))
                process_arrays(typed_cashflow_data, d32);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Date array must contain Date or Date32 values");
        };
        auto dispatch_cashflow = [&](const IColumn * generic_cashflow_data, const IColumn * generic_date_data)
        {
            if (const auto * cf64 = typeid_cast<const ColumnVector<Float64> *>(generic_cashflow_data))
                dispatch_date(cf64, generic_date_data);
            else if (const auto * cf32 = typeid_cast<const ColumnVector<Float32> *>(generic_cashflow_data))
                dispatch_date(cf32, generic_date_data);
            else if (const auto * cu8 = typeid_cast<const ColumnVector<UInt8> *>(generic_cashflow_data))
                dispatch_date(cu8, generic_date_data);
            else if (const auto * cu16 = typeid_cast<const ColumnVector<UInt16> *>(generic_cashflow_data))
                dispatch_date(cu16, generic_date_data);
            else if (const auto * cu32 = typeid_cast<const ColumnVector<UInt32> *>(generic_cashflow_data))
                dispatch_date(cu32, generic_date_data);
            else if (const auto * cu64 = typeid_cast<const ColumnVector<UInt64> *>(generic_cashflow_data))
                dispatch_date(cu64, generic_date_data);
            else if (const auto * ci8 = typeid_cast<const ColumnVector<Int8> *>(generic_cashflow_data))
                dispatch_date(ci8, generic_date_data);
            else if (const auto * ci16 = typeid_cast<const ColumnVector<Int16> *>(generic_cashflow_data))
                dispatch_date(ci16, generic_date_data);
            else if (const auto * ci32 = typeid_cast<const ColumnVector<Int32> *>(generic_cashflow_data))
                dispatch_date(ci32, generic_date_data);
            else if (const auto * ci64 = typeid_cast<const ColumnVector<Int64> *>(generic_cashflow_data))
                dispatch_date(ci64, generic_date_data);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow array must contain numeric values");
        };

        dispatch_cashflow(cashflow_data, date_data);

        return result_col;
    }
};
}

REGISTER_FUNCTION(FunctionXirr)
{
    factory.registerFunction<FunctionXirr>(FunctionDocumentation{
        .description = "Calculates the XIRR (Extended Internal Rate of Return) for a series of cash flows and their corresponding dates."
            " Arrays should be sorted by date in ascending order. Dates need to be unique.",
        .arguments = {
            {"cashflow", "An array of cash flows corresponding to the dates in second param."},
            {"date", "A sorted array of unique dates corresponding to the cash flows."},
        },
        .returned_value = "Returns the XIRR value as a Float64. If the calculation cannot be performed, it returns NaN.",
        .examples = {
            {"simple_example", "SELECT xirr([-10000, 5750, 4250,3250], [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')])","0.6342972615260243"},
        },
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Financial,
    });
}

}
