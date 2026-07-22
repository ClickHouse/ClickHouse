#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/castColumn.h>
#include <boost/math/distributions/normal.hpp>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


class FunctionTwoSampleProportionsZTest : public IFunction
{
public:
    static constexpr auto POOLED = "pooled";
    static constexpr auto UNPOOLED = "unpooled";

    static constexpr auto name = "proportionsZTest";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTwoSampleProportionsZTest>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 6; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {5}; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    static DataTypePtr getReturnType()
    {
        auto float_data_type = std::make_shared<DataTypeNumber<Float64>>();
        DataTypes types(4, float_data_type);

        Strings names{"z_statistic", "p_value", "confidence_interval_low", "confidence_interval_high"};

        return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        for (size_t i = 0; i < 4; ++i)
        {
            if (!isUInt(arguments[i].type))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {}th Argument of function {} must be an unsigned integer.",
                    i + 1,
                    getName());
            }
        }

        if (!isFloat(arguments[4].type))
        {
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The fifth argument {} of function {} should be a float,",
                arguments[4].type->getName(),
                getName()};
        }

        /// There is an additional check for constancy in ExecuteImpl
        if (!isString(arguments[5].type) || !arguments[5].column)
        {
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The sixth argument {} of function {} should be a constant string",
                arguments[5].type->getName(),
                getName()};
        }

        return getReturnType();
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & const_arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto arguments = const_arguments;
        /// Only last argument have to be constant
        for (size_t i = 0; i < 5; ++i)
            arguments[i].column = arguments[i].column->convertToFullColumnIfConst();

        static const auto uint64_data_type = std::make_shared<DataTypeNumber<UInt64>>();

        auto column_successes_x = castColumnAccurate(arguments[0], uint64_data_type);
        const auto & data_successes_x = checkAndGetColumn<ColumnVector<UInt64>>(*column_successes_x).getData();

        auto column_successes_y = castColumnAccurate(arguments[1], uint64_data_type);
        const auto & data_successes_y = checkAndGetColumn<ColumnVector<UInt64>>(*column_successes_y).getData();

        auto column_trials_x = castColumnAccurate(arguments[2], uint64_data_type);
        const auto & data_trials_x = checkAndGetColumn<ColumnVector<UInt64>>(*column_trials_x).getData();

        auto column_trials_y = castColumnAccurate(arguments[3], uint64_data_type);
        const auto & data_trials_y = checkAndGetColumn<ColumnVector<UInt64>>(*column_trials_y).getData();

        static const auto float64_data_type = std::make_shared<DataTypeNumber<Float64>>();

        auto column_confidence_level = castColumnAccurate(arguments[4], float64_data_type);
        const auto & data_confidence_level = checkAndGetColumn<ColumnVector<Float64>>(*column_confidence_level).getData();

        String usevar = checkAndGetColumnConst<ColumnString>(*arguments[5].column).getValue<String>();

        if (usevar != UNPOOLED && usevar != POOLED)
            throw Exception{ErrorCodes::BAD_ARGUMENTS,
                "The sixth argument {} of function {} must be equal to `pooled` or `unpooled`",
                arguments[5].type->getName(),
                getName()};

        const bool is_unpooled = (usevar == UNPOOLED);

        auto res_z_statistic = ColumnFloat64::create();
        auto & data_z_statistic = res_z_statistic->getData();
        data_z_statistic.reserve(input_rows_count);

        auto res_p_value = ColumnFloat64::create();
        auto & data_p_value = res_p_value->getData();
        data_p_value.reserve(input_rows_count);

        auto res_ci_lower = ColumnFloat64::create();
        auto & data_ci_lower = res_ci_lower->getData();
        data_ci_lower.reserve(input_rows_count);

        auto res_ci_upper = ColumnFloat64::create();
        auto & data_ci_upper = res_ci_upper->getData();
        data_ci_upper.reserve(input_rows_count);

        auto insert_values_into_result = [&data_z_statistic, &data_p_value, &data_ci_lower, &data_ci_upper](
                                             Float64 z_stat, Float64 p_value, Float64 lower, Float64 upper)
        {
            data_z_statistic.emplace_back(z_stat);
            data_p_value.emplace_back(p_value);
            data_ci_lower.emplace_back(lower);
            data_ci_upper.emplace_back(upper);
        };

        static constexpr Float64 nan = std::numeric_limits<Float64>::quiet_NaN();

        boost::math::normal_distribution<> nd(0.0, 1.0);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            const UInt64 successes_x = data_successes_x[row_num];
            const UInt64 successes_y = data_successes_y[row_num];
            const UInt64 trials_x = data_trials_x[row_num];
            const UInt64 trials_y = data_trials_y[row_num];
            const Float64 confidence_level = data_confidence_level[row_num];

            const Float64 props_x = static_cast<Float64>(successes_x) / trials_x;
            const Float64 props_y = static_cast<Float64>(successes_y) / trials_y;
            const Float64 diff = props_x - props_y;
            const UInt64 trials_total = trials_x + trials_y;

            if (successes_x == 0 || successes_y == 0 || successes_x > trials_x || successes_y > trials_y || trials_total == 0
                || !std::isfinite(confidence_level) || confidence_level < 0.0 || confidence_level > 1.0)
            {
                insert_values_into_result(nan, nan, nan, nan);
                continue;
            }

            Float64 se = std::sqrt(props_x * (1.0 - props_x) / trials_x + props_y * (1.0 - props_y) / trials_y);

            /// z-statistics
            /// z = \frac{ \bar{p_{1}} - \bar{p_{2}} }{ \sqrt{ \frac{ \bar{p_{1}} \left ( 1 - \bar{p_{1}} \right ) }{ n_{1} } \frac{ \bar{p_{2}} \left ( 1 - \bar{p_{2}} \right ) }{ n_{2} } } }
            Float64 zstat;
            if (is_unpooled)
            {
                zstat = (props_x - props_y) / se;
            }
            else
            {
                UInt64 successes_total = successes_x + successes_y;
                Float64 p_pooled = static_cast<Float64>(successes_total) / trials_total;
                Float64 trials_fact = 1.0 / trials_x + 1.0 / trials_y;
                zstat = diff / std::sqrt(p_pooled * (1.0 - p_pooled) * trials_fact);
            }

            if (unlikely(!std::isfinite(zstat)))
            {
                insert_values_into_result(nan, nan, nan, nan);
                continue;
            }

            // pvalue
            Float64 pvalue = 0;
            Float64 one_side = 1 - boost::math::cdf(nd, std::abs(zstat));
            pvalue = one_side * 2;

            // Confidence intervals
            Float64 d = props_x - props_y;
            Float64 z = -boost::math::quantile(nd, (1.0 - confidence_level) / 2.0);
            Float64 dist = z * se;
            Float64 ci_low = d - dist;
            Float64 ci_high = d + dist;

            insert_values_into_result(zstat, pvalue, ci_low, ci_high);
        }

        return ColumnTuple::create(
            Columns{std::move(res_z_statistic), std::move(res_p_value), std::move(res_ci_lower), std::move(res_ci_upper)});
    }
};


REGISTER_FUNCTION(ZTest)
{
    factory.registerFunction<FunctionTwoSampleProportionsZTest>();
}

}
