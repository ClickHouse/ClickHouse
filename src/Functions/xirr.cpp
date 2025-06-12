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
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>

#include <boost/math/tools/roots.hpp>

#include <algorithm>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

// To add new day count types:
// - add to DayCountType enum
// - add to parseDayCount function
// - add to yearFraction function and implement new date difference logic if necessary
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

enum class IndexMode
{
    ZeroBased, // Cashflows are indexed starting from 0
    OneBased // Cashflows are indexed starting from 1 (Excel style) - option for NPV
};

// NPV function and its derivative. Used for irr calculation
template <typename T>
struct NpvCalculator
{
    using FloatType = std::conditional_t<std::floating_point<T>, T, double>;

    explicit NpvCalculator(std::span<T> cashflows_)
        : cashflows(cashflows_)
    {
        if (cashflows.empty()) [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow array must not be empty");
    }

    template <IndexMode index_mode = IndexMode::ZeroBased>
    double calculate(double rate) const
    {
        if (rate == 0)
            return std::accumulate(cashflows.begin(), cashflows.end(), 0.0);
        if (rate <= -1.0)
            return std::numeric_limits<double>::infinity();

        double npv = 0.0;
        const double growth_factor = 1.0 + rate;
        if constexpr (index_mode == IndexMode::ZeroBased)
        {
            // First cashflow (t=0) is not discounted
            npv = cashflows[0];

            // Discount subsequent cashflows (t=1, t=2, ...)
            double discount_factor = growth_factor; // (1+r)^1
            for (size_t i = 1; i < cashflows.size(); ++i)
            {
                npv += static_cast<FloatType>(cashflows[i]) / discount_factor;
                discount_factor *= growth_factor;
            }
        }
        else
        { // IndexMode::OneBased
            // All cashflows are discounted (t=1, t=2, ...)
            double discount_factor = growth_factor; // Start with (1+r)^1 for t=1
            for (size_t i = 0; i < cashflows.size(); ++i)
            {
                npv += static_cast<FloatType>(cashflows[i]) / discount_factor;
                discount_factor *= growth_factor;
            }
        }
        return npv;
    }

    // Used only for IRR calculation, hence just ZeroBased
    double derivative(double rate) const
    {
        if (rate <= -1.0)
            return std::numeric_limits<double>::quiet_NaN();

        double derivative = 0.0;
        double compound = (1.0 + rate);

        for (size_t i = 1; i < cashflows.size(); ++i)
        {
            compound *= (1.0 + rate);
            derivative += -static_cast<FloatType>(cashflows[i]) * i / compound;
        }
        return derivative;
    }

    std::span<T> cashflows;
};

// NPV function used in the implementation of npv function
template <IndexMode index_mode, typename T>
double npv(double rate, std::span<T> cashflows)
{
    auto calc = NpvCalculator<T>(cashflows);
    return calc.template calculate<index_mode>(rate);
}


// XNPV function and its derivative. Used for xirr calculation
template <typename T, typename D, DayCountType day_count>
struct XnpvCalculator
{
    using FloatType = std::conditional_t<std::floating_point<T>, T, double>;

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
                npv += static_cast<FloatType>(cashflows[i]) / std::pow(1.0 + rate, time);
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
                derivative -= static_cast<FloatType>(cashflows[i]) * time / std::pow(1.0 + rate, time + 1);
        }

        return derivative;
    }

    std::span<T> cashflows;
    std::span<D> dates;
};

// XNPV function used in the implementation of xnpv function
template <DayCountType day_count, typename T, typename D>
double xnpv(double rate, std::span<T> cashflows, std::span<D> dates)
{
    auto calc = XnpvCalculator<T, D, day_count>(cashflows, dates);
    return calc.calculate(rate);
}


enum class SolverErrorCode
{
    CANNOT_EVALUATE_VALUE,
    CANNOT_CONVERGE_DUE_TO_ROUNDING_ERRORS,
    CANNOT_CONVERGE_DUE_TO_INVALID_ARGUMENTS,
    CANNOT_CONVERGE_TOO_MANY_ITERATIONS,
    INPUT_DATES_NOT_SORTED_UNIQUE,
    NO_ROOT_FOUND_IN_BRACKET,
    OTHER_ERROR
};

template <typename Function, typename Derivative>
std::expected<double, SolverErrorCode> solver(Function && fun, Derivative && der, double guess)
{
    constexpr int max_iterations = 100;
    constexpr double start_lower_bound = -0.999999; // Avoid the rate of -1.
    constexpr double start_upper_bound = 100.0; // Reasonable upper bound for financial applications IRR/XIRR
    constexpr double tolerance = 1e-6; // Tolerance for the result check
    try
    {
        boost::uintmax_t max_iter = max_iterations;

        double result = boost::math::tools::newton_raphson_iterate(
            [&fun, &der](double x) { return std::make_tuple(fun(x), der(x)); },
            guess,
            start_lower_bound,
            start_upper_bound,
            std::numeric_limits<double>::digits - 4,
            max_iter);

        if (result >= start_lower_bound && result <= start_upper_bound && std::abs(fun(result)) < tolerance)
            return result;

        // Fallback to TOMS748
        const double f_lower = fun(start_lower_bound);
        const double f_upper = fun(start_upper_bound);

        if (f_lower * f_upper >= 0.0)
            return std::unexpected(SolverErrorCode::NO_ROOT_FOUND_IN_BRACKET);

        max_iter = max_iterations;
        boost::math::tools::eps_tolerance<double> tol(std::numeric_limits<double>::digits - 4);
        auto toms_result = boost::math::tools::toms748_solve(fun, start_lower_bound, start_upper_bound, tol, max_iter);

        return toms_result.first;
    }
    catch (const boost::math::evaluation_error &)
    {
        return std::unexpected(SolverErrorCode::CANNOT_EVALUATE_VALUE);
    }
    catch (const boost::math::rounding_error &)
    {
        return std::unexpected(SolverErrorCode::CANNOT_CONVERGE_DUE_TO_ROUNDING_ERRORS);
    }
    catch (const std::domain_error &)
    {
        return std::unexpected(SolverErrorCode::CANNOT_CONVERGE_DUE_TO_INVALID_ARGUMENTS);
    }
    catch (...)
    {
        return std::unexpected(SolverErrorCode::OTHER_ERROR);
    }
}

template <DayCountType day_count, typename T, typename D>
std::expected<double, SolverErrorCode> calculateXirr(std::span<T> cashflows, std::span<D> dates, double guess)
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
            return std::unexpected(SolverErrorCode::INPUT_DATES_NOT_SORTED_UNIQUE);
    }

    auto xnpv = XnpvCalculator<T, D, day_count>(cashflows, dates);

    auto xnpv_function = [&](double rate) { return xnpv.calculate(rate); };
    auto xnpv_derivative = [&](double rate) { return xnpv.derivative(rate); };

    return solver(xnpv_function, xnpv_derivative, guess);
}

template <typename T>
std::expected<double, SolverErrorCode> calculateIrr(std::span<T> cashflows, double guess)
{
    if (cashflows.size() <= 1) [[unlikely]]
        return std::numeric_limits<double>::quiet_NaN();

    bool any_positive = std::any_of(cashflows.begin(), cashflows.end(), [](T cf) { return cf > 0; });
    bool any_negative = std::any_of(cashflows.begin(), cashflows.end(), [](T cf) { return cf < 0; });

    if (!(any_negative && any_positive)) [[unlikely]]
        return std::numeric_limits<double>::quiet_NaN();

    auto npv = NpvCalculator<T>(cashflows);

    auto npv_function = [&](double rate) { return npv.calculate(rate); };
    auto npv_derivative = [&](double rate) { return npv.derivative(rate); };

    return solver(npv_function, npv_derivative, guess);
}

bool isCashFlowColumn(const IDataType & type)
{
    return isArray(type) && isNativeNumber(checkAndGetDataType<DataTypeArray>(type).getNestedType());
}

bool isXirrDateColumn(const IDataType & type)
{
    return isArray(type) && isDateOrDate32(checkAndGetDataType<DataTypeArray>(type).getNestedType());
}

// Similar dispatch is needed in two of the functions below, so we define it here
template <typename T, typename F>
void dispatchDate(const T * cashflow_data, const IColumn * date_data, F && f)
{
    if (const auto * d = typeid_cast<const ColumnDate *>(date_data))
        f(cashflow_data, d);
    else if (const auto * d32 = typeid_cast<const ColumnDate32 *>(date_data))
        f(cashflow_data, d32);
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Date array must contain Date or Date32 values");
}

template <typename F>
void dispatchCashflowDate(const IColumn * cashflow_data, const IColumn * date_data, F && f)
{
    if (const auto * cf64 = typeid_cast<const ColumnVector<Float64> *>(cashflow_data))
        dispatchDate(cf64, date_data, std::forward<F>(f));
    else if (const auto * cf32 = typeid_cast<const ColumnVector<Float32> *>(cashflow_data))
        dispatchDate(cf32, date_data, std::forward<F>(f));
    else if (const auto * ci8 = typeid_cast<const ColumnVector<Int8> *>(cashflow_data))
        dispatchDate(ci8, date_data, std::forward<F>(f));
    else if (const auto * ci16 = typeid_cast<const ColumnVector<Int16> *>(cashflow_data))
        dispatchDate(ci16, date_data, std::forward<F>(f));
    else if (const auto * ci32 = typeid_cast<const ColumnVector<Int32> *>(cashflow_data))
        dispatchDate(ci32, date_data, std::forward<F>(f));
    else if (const auto * ci64 = typeid_cast<const ColumnVector<Int64> *>(cashflow_data))
        dispatchDate(ci64, date_data, std::forward<F>(f));
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow array must contain Float64/Float32/Int64/Int32/Int16/Int8 values");
}

class FunctionXirr : public IFunction
{
public:
    static constexpr auto name = "xirr";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionXirr>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"cashflow",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn),
             nullptr,
             "Array[Float64|Float32|Int64|Int32|Int16|Int8]"},
            {"date", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isXirrDateColumn), nullptr, "Array[Date/Date32]"},
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

        const ColumnArray::Offsets & cashflow_offsets = cashflow_array->getOffsets();
        const ColumnArray::Offsets & date_offsets = date_array->getOffsets();
        if (cashflow_offsets.size() != date_offsets.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same number of rows");

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

        auto result_col = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = result_col->getData();

        auto process_arrays
            = [&]<typename CashFlowCol, typename DateTypeCol>(const CashFlowCol * cashflow_values, const DateTypeCol * date_values)
        {
            using CashFlowType = const typename CashFlowCol::ValueType;
            using DateType = const typename DateTypeCol::ValueType;

            auto loop = [&]<DayCountType day_count_type>
            {
                ColumnArray::Offset previous_offset = 0;
                for (size_t i = 0; i < cashflow_offsets.size(); ++i)
                {
                    const auto current_offset = cashflow_offsets[i];
                    if (current_offset != date_offsets[i])
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size for each row");

                    const auto length = current_offset - previous_offset;
                    auto cashflow_span = std::span<CashFlowType>(cashflow_values->getData().data() + previous_offset, length);
                    auto date_span = std::span<DateType>(date_values->getData().data() + previous_offset, length);

                    auto xirr = calculateXirr<day_count_type, CashFlowType, DateType>(cashflow_span, date_span, guess);
                    if (xirr.has_value()) [[likely]]
                        result_data[i] = xirr.value();
                    else
                        result_data[i] = std::numeric_limits<double>::quiet_NaN();

                    previous_offset = current_offset;
                }
            };
            switch (day_count)
            {
                case DayCountType::ACT_365F:
                    return loop.template operator()<DayCountType::ACT_365F>();
                case DayCountType::ACT_365_25:
                    return loop.template operator()<DayCountType::ACT_365_25>();
            }
        };

        const auto * cashflow_data = &cashflow_array->getData();
        const auto * date_data = &date_array->getData();
        dispatchCashflowDate(cashflow_data, date_data, process_arrays);

        return result_col;
    }
};

class FunctionIRR : public IFunction
{
public:
    static constexpr auto name = "irr";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIRR>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"cashflow",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn),
             nullptr,
             "Array[Float64|Float32|Int64|Int32|Int16|Int8]"},
        };

        auto optional_args = FunctionArgumentDescriptors{
            {"guess", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "FloatXX"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cashflow_col = arguments[0].column->convertToFullColumnIfConst();
        const auto * cashflow_array = checkAndGetColumn<ColumnArray>(cashflow_col.get());
        if (!cashflow_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow argument must be an array");
        const ColumnArray::Offsets & cashflow_offsets = cashflow_array->getOffsets();

        double guess = 0.1;
        if (arguments.size() > 1)
        {
            if (!isColumnConst(*arguments[1].column))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument (guess) must be a constant");
            guess = arguments[1].column->getFloat64(0);
        }

        auto result_col = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = result_col->getData();
        auto process_array = [&](const auto * cashflow_values)
        {
            ColumnArray::Offset previous_offset = 0;
            for (size_t i = 0; i < cashflow_offsets.size(); ++i)
            {
                const auto current_offset = cashflow_offsets[i];
                const auto length = current_offset - previous_offset;

                if (length <= 1) [[unlikely]]
                {
                    result_data[i] = std::numeric_limits<double>::quiet_NaN();
                    previous_offset = current_offset;
                    continue;
                }

                auto cashflow_span = std::span(cashflow_values->getData().data() + previous_offset, length);
                auto irr_result = calculateIrr(cashflow_span, guess);

                if (irr_result.has_value())
                    result_data[i] = irr_result.value();
                else
                    result_data[i] = std::numeric_limits<double>::quiet_NaN();

                previous_offset = current_offset;
            }
        };

        const auto * cashflow_data = &cashflow_array->getData();
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

class FunctionXnpv : public IFunction
{
public:
    static constexpr auto name = "xnpv";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionXnpv>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"guess", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "FloatXX"},
            {"cashflow",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn),
             nullptr,
             "Array[Float64|Float32|Int64|Int32|Int16|Int8]"},
            {"date", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isXirrDateColumn), nullptr, "Array[Date|Date32]"},
        };

        auto optional_args = FunctionArgumentDescriptors{
            {"daycount", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto rate_col = arguments[0].column->convertToFullColumnIfConst();
        auto cashflow_col = arguments[1].column->convertToFullColumnIfConst();
        auto date_col = arguments[2].column->convertToFullColumnIfConst();

        const auto * cashflow_array = checkAndGetColumn<ColumnArray>(cashflow_col.get());
        const auto * date_array = checkAndGetColumn<ColumnArray>(date_col.get());

        if (!cashflow_array || !date_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Both cashflow and date arguments must be arrays");

        const ColumnArray::Offsets & cashflow_offsets = cashflow_array->getOffsets();
        const ColumnArray::Offsets & date_offsets = date_array->getOffsets();
        if (cashflow_offsets.size() != date_offsets.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same number of rows");

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

        auto result_col = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = result_col->getData();

        auto process_arrays = [&]<typename CashFlowCol, typename DateTypeCol>(
                                  const CashFlowCol * cashflow_values, const DateTypeCol * date_values, const auto & rate_pod)
        {
            using CashFlowType = const typename CashFlowCol::ValueType;
            using DateType = const typename DateTypeCol::ValueType;
            auto loop = [&]<DayCountType day_count_type>
            {
                ColumnArray::Offset previous_offset = 0;
                for (size_t i = 0; i < cashflow_offsets.size(); ++i)
                {
                    const auto current_offset = cashflow_offsets[i];
                    if (current_offset != date_offsets[i])
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cashflow and date arrays must have the same size for each row");
                    const auto rate = rate_pod[i];

                    const auto length = current_offset - previous_offset;
                    auto cashflow_span = std::span<CashFlowType>(cashflow_values->getData().data() + previous_offset, length);
                    auto date_span = std::span<DateType>(date_values->getData().data() + previous_offset, length);

                    result_data[i] = xnpv<day_count_type, CashFlowType, DateType>(rate, cashflow_span, date_span);

                    previous_offset = current_offset;
                }
            };
            switch (day_count)
            {
                case DayCountType::ACT_365F:
                    return loop.template operator()<DayCountType::ACT_365F>();
                case DayCountType::ACT_365_25:
                    return loop.template operator()<DayCountType::ACT_365_25>();
            }
        };

        auto dispatch = [&](const auto * cashflow_data, const auto * date_data)
        {
            if (const auto * rate_f64 = checkAndGetColumn<ColumnVector<Float64>>(rate_col.get()))
                process_arrays(cashflow_data, date_data, rate_f64->getData());
            else if (const auto * rate_f32 = checkAndGetColumn<ColumnVector<Float32>>(rate_col.get()))
                process_arrays(cashflow_data, date_data, rate_f32->getData());
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Rate argument must be a FloatXX column");
        };

        const auto * cashflow_data = &cashflow_array->getData();
        const auto * date_data = &date_array->getData();
        dispatchCashflowDate(cashflow_data, date_data, dispatch);

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
            {"cashflow",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isCashFlowColumn),
             nullptr,
             "Array[Float64|Float32|Int64|Int32|Int16|Int8]"},
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

        auto process_array = [&]<typename CashFlowCol>(const CashFlowCol * cashflow_values, const auto & rate_pod)
        {
            using CashFlowType = const typename CashFlowCol::ValueType;
            auto loop = [&]<IndexMode index_mode>()
            {
                ColumnArray::Offset previous_offset = 0;
                for (size_t i = 0; i < cashflow_offsets.size(); ++i)
                {
                    const auto current_offset = cashflow_offsets[i];
                    const auto length = current_offset - previous_offset;
                    const auto rate = rate_pod[i];
                    auto cashflow_span = std::span(cashflow_values->getData().data() + previous_offset, length);

                    result_data[i] = npv<index_mode, CashFlowType>(rate, cashflow_span);

                    previous_offset = current_offset;
                }
            };

            if (start_from_zero)
                loop.template operator()<IndexMode::ZeroBased>();
            else
                loop.template operator()<IndexMode::OneBased>();
        };

        auto dispatch = [&](const auto * cashflow_data)
        {
            if (const auto * rate_f64 = checkAndGetColumn<ColumnVector<Float64>>(rate_col.get()))
                process_array(cashflow_data, rate_f64->getData());
            else if (const auto * rate_f32 = checkAndGetColumn<ColumnVector<Float32>>(rate_col.get()))
                process_array(cashflow_data, rate_f32->getData());
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Rate argument must be a FloatXX column");
        };

        const auto * cashflow_data = &cashflow_array->getData();

        if (const auto * cf64 = typeid_cast<const ColumnVector<Float64> *>(cashflow_data))
            dispatch(cf64);
        else if (const auto * cf32 = typeid_cast<const ColumnVector<Float32> *>(cashflow_data))
            dispatch(cf32);
        else if (const auto * ci8 = typeid_cast<const ColumnVector<Int8> *>(cashflow_data))
            dispatch(ci8);
        else if (const auto * ci16 = typeid_cast<const ColumnVector<Int16> *>(cashflow_data))
            dispatch(ci16);
        else if (const auto * ci32 = typeid_cast<const ColumnVector<Int32> *>(cashflow_data))
            dispatch(ci32);
        else if (const auto * ci64 = typeid_cast<const ColumnVector<Int64> *>(cashflow_data))
            dispatch(ci64);
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

REGISTER_FUNCTION(FunctionIRR)
{
    factory.registerFunction<FunctionIRR>(FunctionDocumentation{
        .description = "Calculates the IRR (Internal Rate of Return) for a series of cash flows.",
        .arguments = {
            {"cashflow", "An array of cash flows."},
            {"guess", "An optional guess value for the IRR calculation. Default is 0.1."},
        },
        .returned_value = "Returns the IRR value as a Float64. If the calculation cannot be performed, it returns NaN.",
        .examples = {
            {"simple_example", "SELECT irr([-100, 39, 59, 55, 20])", "0.2809484212526239"},
            {"simple_example_with_default", "SELECT irr([-100, 39, 59, 55, 20], 0.1)", "0.2809484212526239"},
        },
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Financial,
    });
}

REGISTER_FUNCTION(FunctionXnpv)
{
    factory.registerFunction<FunctionXnpv>(FunctionDocumentation{
        .description = "Calculates the XNPV (Extended Net Present Value) for a series of cash flows and their corresponding dates.",
        .arguments = {
            {"rate", "The discount rate as a FloatXX."},
            {"cashflow", "An array of cash flows."},
            {"date", "An array of dates corresponding to the cash flows."},
        },
        .returned_value = "Returns the XNPV value as a Float64.",
        .examples = {
            {"simple_example", "SELECT xnpv(0.1, [-10_000., 5750., 4250., 3250.], [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')])", "3065.2226681795255"},
            {"simple_example", "SELECT xnpv(0.1, [-10_000., 5750., 4250., 3250.], [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')], 'ACT_365_25')", "2507.067268742502"},
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
