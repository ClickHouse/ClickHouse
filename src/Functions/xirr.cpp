#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/IColumn.h>
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
#include "Common/Exception.h"
#include <Common/FunctionDocumentation.h>

#include <boost/math/tools/roots.hpp>

#include <algorithm>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

enum class DayCountType
{
    ACT_365F,
    ACT_365_25,
};

std::optional<DayCountType> parseDayCount(std::string_view day_count)
{
    using enum DayCountType;
    if (day_count == "ACT_365F")
        return ACT_365F;
    if (day_count == "ACT_365_25")
        return ACT_365_25;
    return std::nullopt;
}

constexpr int daysBetweenAct(int d1, int d2)
{
    return d2 - d1;
}

template <DayCountType day_count, typename D>
constexpr double yearFraction(D d1, D d2)
{
    if constexpr (day_count == DayCountType::ACT_365F)
        return daysBetweenAct(d1, d2) / 365.0;
    else if constexpr (day_count == DayCountType::ACT_365_25)
        return daysBetweenAct(d1, d2) / 365.25;
    else
        []<bool flag = false>() { static_assert(flag, "Unsupported DayCountType"); }();
}

template <typename T>
double npv(double rate, std::span<T> cashflows, bool start_from_zero)
{
    if (rate == 0)
        return std::accumulate(cashflows.begin(), cashflows.end(), 0.0);
    if (rate <= -1.0)
        return std::numeric_limits<double>::infinity();
    double npv_value = 0.0;
    for (std::size_t idx = 0; idx < cashflows.size(); ++idx)
    {
        const double i = static_cast<double>(idx) + (start_from_zero ? 0.0 : 1.0);
        npv_value += cashflows[idx] / std::pow(1.0 + rate, i);
    }
    return npv_value;
}

template <typename T, typename D, DayCountType day_count>
struct XnpvCalculator
{
    XnpvCalculator(std::span<T> cashflows_, std::span<D> dates_)
        : cashflows(cashflows_)
        , dates(dates_)
    {
        if (cashflows.size() != dates.size()) [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size");
    }

    double calculate(double rate) const
    {
        if (rate <= -1.0)
            return std::numeric_limits<double>::infinity();

        double npv = 0.0;

        for (size_t i = 0; i < cashflows.size(); ++i)
        {
            double time = yearFraction<day_count>(dates[0], dates[i]);
            if (time == 0.0)
                npv += cashflows[i];
            else
                npv += cashflows[i] / std::pow(1.0 + rate, time);
        }

        return npv;
    }

    double derivative(double rate) const
    {
        if (rate <= -1.0)
            return std::numeric_limits<double>::infinity();

        double derivative = 0.0;

        for (size_t i = 0; i < cashflows.size(); ++i)
        {
            double time = yearFraction<day_count>(dates[0], dates[i]);
            if (time != 0.0)
                derivative -= cashflows[i] * time / std::pow(1.0 + rate, time + 1);
        }

        return derivative;
    }

    std::span<T> cashflows;
    std::span<D> dates;
};

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

    template <DayCountType day_count, typename T, typename D>
    static std::expected<double, XirrErrorCode> calculateXirr(std::span<T> cashflows, std::span<D> dates, double guess)
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

        auto npv = XnpvCalculator<T, D, day_count>(cashflows, dates);

        auto npv_function = [&](double rate) { return npv.calculate(rate); };
        auto npv_derivative = [&](double rate) { return npv.derivative(rate); };

        try
        {
            boost::uintmax_t max_iter = MAX_ITERATIONS;

            double result = boost::math::tools::newton_raphson_iterate(
                [&](double x) { return std::make_tuple(npv_function(x), npv_derivative(x)); },
                guess,
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

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"cashflow", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn), nullptr, "Array[Number]"},
            {"date", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isXirrDateColumn), nullptr, "Array[NativeNumber]"},
        };

        auto optional_args = FunctionArgumentDescriptors{
            {"guess", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "FloatXX"},
            {"daycount", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

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

        double guess = 0.1;
        if (arguments.size() > 2)
        {
            if (!isColumnConst(*arguments[2].column))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument (guess) must be a constant");
            guess = arguments[2].column->getFloat64(0);
        }

        DayCountType day_count = DayCountType::ACT_365F;
        if (arguments.size() > 3)
        {
            if (!isColumnConst(*arguments[3].column) || !isString(arguments[3].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Fourth argument (daycount) must be a constant string");
            auto day_count_str = arguments[3].column->getDataAt(0).toString();
            auto parsed_day_count = parseDayCount(day_count_str);
            if (!parsed_day_count.has_value())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid day count type: {}", day_count_str);
            day_count = parsed_day_count.value();
        }

        auto process_arrays = [&]<typename CashFlowCol, typename DateTypeCol>(const CashFlowCol * cashflow_values, const DateTypeCol * date_values)
        {
            using CashFlowType = const typename CashFlowCol::ValueType;
            using DateType = const typename DateTypeCol::ValueType;
            auto * xirr_fptr = [&]() -> std::expected<double, XirrCalculator::XirrErrorCode>(*)(std::span<CashFlowType>, std::span<DateType>, double)
            {
                switch (day_count)
                {
                case DayCountType::ACT_365F:
                    return &XirrCalculator::calculateXirr<DayCountType::ACT_365F>;
                case DayCountType::ACT_365_25:
                    return &XirrCalculator::calculateXirr<DayCountType::ACT_365_25>;
                }
            }();

            ColumnArray::Offset previous_offset = 0;
            for (size_t i = 0; i < cashflow_offsets.size(); ++i)
            {
                const auto current_offset = cashflow_offsets[i];
                if (current_offset != date_offsets[i])
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size for each row");

                const auto length = current_offset - previous_offset;
                auto cashflow_span = std::span<CashFlowType>(cashflow_values->getData().data() + previous_offset, length);
                auto date_span = std::span<DateType>(date_values->getData().data() + previous_offset, length);

                auto xirr = xirr_fptr(cashflow_span, date_span, guess);
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

class FunctionNPV : public IFunction
{
public:
    static constexpr auto name = "npv";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNPV>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"rate", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "FloatXX"},
            {"cashflow", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn), nullptr, "Array[Number]"},
        };

        auto optional_args = FunctionArgumentDescriptors{
            {"start_from_zero", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Bool"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto rate_col = arguments[0].column->convertToFullColumnIfConst();
        auto cashflow_col = arguments[1].column->convertToFullColumnIfConst();

        const auto * cashflow_array = checkAndGetColumn<ColumnArray>(cashflow_col.get());
        if (!cashflow_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow argument must be an array");

        const auto * rate_f64 = checkAndGetColumn<ColumnVector<Float64>>(rate_col.get());
        if (!rate_f64)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Rate argument must be a Float64 column");
        const auto & rate_pod = rate_f64->getData();

        bool start_from_zero = true;
        if (arguments.size() > 2)
        {
            if (!isColumnConst(*arguments[2].column) || !isInteger(arguments[2].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument (start_from_zero) must be a constant boolean");
            start_from_zero = arguments[2].column->getUInt(0) != 0;
        }

        auto result_col = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = result_col->getData();

        const ColumnArray::Offsets & cashflow_offsets = cashflow_array->getOffsets();
        const auto * cashflow_data = &cashflow_array->getData();

        auto process_array = [&](const auto * cashflow_values)
        {
            ColumnArray::Offset previous_offset = 0;
            for (size_t i = 0; i < cashflow_offsets.size(); ++i)
            {
                const auto current_offset = cashflow_offsets[i];
                const auto length = current_offset - previous_offset;
                const auto rate = rate_pod[i];
                auto cashflow_span = std::span(cashflow_values->getData().data() + previous_offset, length);

                result_data[i] = npv(rate, cashflow_span, start_from_zero);

                previous_offset = current_offset;
            }
        };
        if (const auto * cf64 = typeid_cast<const ColumnVector<Float64> *>(cashflow_data))
            process_array(cf64);
        else if (const auto * cf32 = typeid_cast<const ColumnVector<Float32> *>(cashflow_data))
            process_array(cf32);
        else if (const auto * ci8 = typeid_cast<const ColumnVector<Int8> *>(cashflow_data))
            process_array(ci8);
        else if (const auto * ci16 = typeid_cast<const ColumnVector<Int16> *>(cashflow_data))
            process_array(ci16);
        else if (const auto * ci32 = typeid_cast<const ColumnVector<Int32> *>(cashflow_data))
            process_array(ci32);
        else if (const auto * ci64 = typeid_cast<const ColumnVector<Int64> *>(cashflow_data))
            process_array(ci64);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow array must contain numeric values");

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
            {"simple_example_with_guess", "SELECT xirr([-10000, 5750, 4250,3250], [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')], 0.5)","0.6342972615260243"},
            {"simple_example_daycount", "SELECT round(xirr([100000, -110000], [toDate('2020-01-01'), toDate('2021-01-01')], 0.1, 'ACT_365_25'), 6) AS xirr_365_25;", "0.099785"},
        },
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Financial,
    });
}

REGISTER_FUNCTION(FunctionNPV)
{
    factory.registerFunction<FunctionNPV>(FunctionDocumentation{
        .description = "Calculates the Net Present Value (NPV) of a series of cash flows given a discount rate.",
        .arguments = {
            {"rate", "The discount rate as a FloatXX."},
            {"cashflow", "An array of cash flows."},
            {"start_from_zero", "A boolean indicating whether to start the NPV calculation from zero. Default is true."},
        },
        .returned_value = "Returns the NPV value as a Float64.",
        .examples = {
            {"simple_example", "SELECT npv(0.08, [-40_000., 5_000., 8_000., 12_000., 30_000.])", "3065.2226681795255"},
            {"simple_example_exel", "SELECT npv(0.08, [-40_000., 5_000., 8_000., 12_000., 30_000.], False)", "2838.1691372032656"},
        },
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Financial,
    });
}

}

