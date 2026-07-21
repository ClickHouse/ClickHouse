#include <cfloat>
#include <cmath>

#include <boost/math/distributions/normal.hpp>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Impl>
class FunctionMinSampleSize : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMinSampleSize<Impl>>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return Impl::num_args; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return ColumnNumbers(std::begin(Impl::const_args), std::end(Impl::const_args));
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    static DataTypePtr getReturnType()
    {
        auto float_64_type = std::make_shared<DataTypeNumber<Float64>>();

        DataTypes types{
            float_64_type,
            float_64_type,
            float_64_type,
        };

        Strings names{
            "minimum_sample_size",
            "detect_range_lower",
            "detect_range_upper",
        };

        return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        Impl::validateArguments(arguments);
        return getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return Impl::execute(arguments, input_rows_count);
    }
};

static bool isBetweenZeroAndOne(Float64 v)
{
    return v >= 0.0 && v <= 1.0 && fabs(v - 0.0) >= DBL_EPSILON && fabs(v - 1.0) >= DBL_EPSILON;
}

struct ContinuousImpl
{
    static constexpr auto name = "minSampleSizeContinuous";
    static constexpr size_t num_args = 5;
    static constexpr size_t const_args[] = {2, 3, 4};

    static void validateArguments(const DataTypes & arguments)
    {
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!isNativeNumber(arguments[i]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a number.", i + 1, name);
            }
        }
    }

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto float_64_type = std::make_shared<DataTypeFloat64>();
        auto baseline_argument = arguments[0];
        baseline_argument.column = baseline_argument.column->convertToFullColumnIfConst();
        auto baseline_column_untyped = castColumnAccurate(baseline_argument, float_64_type);
        const auto & baseline_column = checkAndGetColumn<ColumnVector<Float64>>(*baseline_column_untyped);
        const auto & baseline_column_data = baseline_column.getData();

        auto sigma_argument = arguments[1];
        sigma_argument.column = sigma_argument.column->convertToFullColumnIfConst();
        auto sigma_column_untyped = castColumnAccurate(sigma_argument, float_64_type);
        const auto & sigma_column = checkAndGetColumn<ColumnVector<Float64>>(*sigma_column_untyped);
        const auto & sigma_column_data = sigma_column.getData();

        const IColumn & col_mde = *arguments[2].column;
        const IColumn & col_power = *arguments[3].column;
        const IColumn & col_alpha = *arguments[4].column;

        auto res_min_sample_size = ColumnFloat64::create();
        auto & data_min_sample_size = res_min_sample_size->getData();
        data_min_sample_size.reserve(input_rows_count);

        auto res_detect_lower = ColumnFloat64::create();
        auto & data_detect_lower = res_detect_lower->getData();
        data_detect_lower.reserve(input_rows_count);

        auto res_detect_upper = ColumnFloat64::create();
        auto & data_detect_upper = res_detect_upper->getData();
        data_detect_upper.reserve(input_rows_count);

        /// Minimal Detectable Effect
        const Float64 mde = col_mde.getFloat64(0);
        /// Sufficient statistical power to detect a treatment effect
        const Float64 power = col_power.getFloat64(0);
        /// Significance level
        const Float64 alpha = col_alpha.getFloat64(0);

        boost::math::normal_distribution<> nd(0.0, 1.0);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            /// Mean of control-metric
            Float64 baseline = baseline_column_data[row_num];
            /// Standard deviation of conrol-metric
            Float64 sigma = sigma_column_data[row_num];

            if (!std::isfinite(baseline) || !std::isfinite(sigma) || !isBetweenZeroAndOne(mde) || !isBetweenZeroAndOne(power)
                || !isBetweenZeroAndOne(alpha))
            {
                data_min_sample_size.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                data_detect_lower.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                data_detect_upper.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                continue;
            }

            Float64 delta = baseline * mde;

            using namespace boost::math;
            /// https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a
            /// \frac{2\sigma^{2} * (Z_{1 - alpha /2} + Z_{power})^{2}}{\Delta^{2}}
            Float64 min_sample_size
                = 2 * std::pow(sigma, 2) * std::pow(quantile(nd, 1.0 - alpha / 2) + quantile(nd, power), 2) / std::pow(delta, 2);

            data_min_sample_size.emplace_back(min_sample_size);
            data_detect_lower.emplace_back(baseline - delta);
            data_detect_upper.emplace_back(baseline + delta);
        }

        return ColumnTuple::create(Columns{std::move(res_min_sample_size), std::move(res_detect_lower), std::move(res_detect_upper)});
    }
};


struct ConversionImpl
{
    static constexpr auto name = "minSampleSizeConversion";
    static constexpr size_t num_args = 4;
    static constexpr size_t const_args[] = {1, 2, 3};

    static void validateArguments(const DataTypes & arguments)
    {
        size_t arguments_size = arguments.size();
        for (size_t i = 0; i < arguments_size; ++i)
        {
            if (!isFloat(arguments[i]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th argument of function {} must be a float.", i + 1, name);
            }
        }
    }

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto first_argument_column = castColumnAccurate(arguments[0], std::make_shared<DataTypeFloat64>());

        if (const ColumnConst * const col_p1_const = checkAndGetColumnConst<ColumnVector<Float64>>(first_argument_column.get()))
        {
            const Float64 left_value = col_p1_const->template getValue<Float64>();
            return process<true>(arguments, &left_value, input_rows_count);
        }
        if (const ColumnVector<Float64> * const col_p1 = checkAndGetColumn<ColumnVector<Float64>>(first_argument_column.get()))
        {
            return process<false>(arguments, col_p1->getData().data(), input_rows_count);
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must be a float.", name);
    }

    template <bool const_p1>
    static ColumnPtr process(const ColumnsWithTypeAndName & arguments, const Float64 * col_p1, const size_t input_rows_count)
    {
        const IColumn & col_mde = *arguments[1].column;
        const IColumn & col_power = *arguments[2].column;
        const IColumn & col_alpha = *arguments[3].column;

        auto res_min_sample_size = ColumnFloat64::create();
        auto & data_min_sample_size = res_min_sample_size->getData();
        data_min_sample_size.reserve(input_rows_count);

        auto res_detect_lower = ColumnFloat64::create();
        auto & data_detect_lower = res_detect_lower->getData();
        data_detect_lower.reserve(input_rows_count);

        auto res_detect_upper = ColumnFloat64::create();
        auto & data_detect_upper = res_detect_upper->getData();
        data_detect_upper.reserve(input_rows_count);

        /// Minimal Detectable Effect
        const Float64 mde = col_mde.getFloat64(0);
        /// Sufficient statistical power to detect a treatment effect
        const Float64 power = col_power.getFloat64(0);
        /// Significance level
        const Float64 alpha = col_alpha.getFloat64(0);

        boost::math::normal_distribution<> nd(0.0, 1.0);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            /// Proportion of control-metric
            Float64 p1;

            if constexpr (const_p1)
            {
                p1 = col_p1[0];
            }
            else if constexpr (!const_p1)
            {
                p1 = col_p1[row_num];
            }

            if (!std::isfinite(p1) || !isBetweenZeroAndOne(mde) || !isBetweenZeroAndOne(power) || !isBetweenZeroAndOne(alpha))
            {
                data_min_sample_size.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                data_detect_lower.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                data_detect_upper.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                continue;
            }

            Float64 q1 = 1.0 - p1;
            Float64 p2 = p1 + mde;
            Float64 q2 = 1.0 - p2;
            Float64 p_bar = (p1 + p2) / 2.0;
            Float64 q_bar = 1.0 - p_bar;

            using namespace boost::math;
            /// https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a
            /// \frac{(Z_{1-alpha/2} * \sqrt{2*\bar{p}*\bar{q}} + Z_{power} * \sqrt{p1*q1+p2*q2})^{2}}{\Delta^{2}}
            Float64 min_sample_size
                = std::pow(
                      quantile(nd, 1.0 - alpha / 2.0) * std::sqrt(2.0 * p_bar * q_bar) + quantile(nd, power) * std::sqrt(p1 * q1 + p2 * q2),
                      2)
                / std::pow(mde, 2);

            data_min_sample_size.emplace_back(min_sample_size);
            data_detect_lower.emplace_back(p1 - mde);
            data_detect_upper.emplace_back(p1 + mde);
        }

        return ColumnTuple::create(Columns{std::move(res_min_sample_size), std::move(res_detect_lower), std::move(res_detect_upper)});
    }
};


REGISTER_FUNCTION(MinSampleSize)
{
    factory.registerFunction<FunctionMinSampleSize<ContinuousImpl>>();
    /// Needed for backward compatibility
    factory.registerAlias("minSampleSizeContinous", FunctionMinSampleSize<ContinuousImpl>::name);
    factory.registerFunction<FunctionMinSampleSize<ConversionImpl>>();
}

}
