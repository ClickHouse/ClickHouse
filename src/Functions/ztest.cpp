#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <boost/math/distributions/normal.hpp>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }


    class FunctionTwoSampleProportionsZTest : public IFunction
    {
    public:
        static constexpr auto POOLED = "pooled";
        static constexpr auto UNPOOLED  = "unpooled";

        static constexpr auto name = "proportionsZTest";

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionTwoSampleProportionsZTest>();
        }

        String getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 6; }

        bool useDefaultImplementationForNulls() const override { return false; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        static DataTypePtr getReturnType()
        {
            DataTypes types
            {
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };

            Strings names
            {
                "z_statistic",
                "p_value",
                "confidence_interval_low",
                "confidence_interval_high"
            };

            return std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            );
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
        {
            return getReturnType();
        }

        static void nan(MutableColumnPtr & to)
        {
            const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
            Tuple tuple({nan, nan, nan, nan});
            to->insert(tuple);
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            MutableColumnPtr to{getReturnType()->createColumn()};
            to->reserve(input_rows_count);

            if (!isString(arguments[5].type))
                throw Exception{"The sixth argument of function " + getName() + " should be String, illegal type: " + arguments[5].type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            const ColumnPtr & col_usevar = arguments[5].column;

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                const UInt64 successes_x = arguments[0].column->getUInt(row_num);
                const UInt64 successes_y = arguments[1].column->getUInt(row_num);
                const UInt64 trials_x = arguments[2].column->getUInt(row_num);
                const UInt64 trials_y = arguments[3].column->getUInt(row_num);
                const Float64 props_x = static_cast<Float64>(successes_x) / trials_x;
                const Float64 props_y = static_cast<Float64>(successes_y) / trials_y;
                const Float64 diff = props_x - props_y;
                const UInt64 trials_total = trials_x + trials_y;
                const Float64 confidence_level = arguments[4].column->getFloat64(row_num);

                if (successes_x == 0 || successes_y == 0
                    || successes_x > trials_x || successes_y > trials_y
                    || trials_total == 0
                    || !std::isfinite(confidence_level) || confidence_level < 0.0 || confidence_level > 1.0)
                {
                    nan(to);
                    continue;
                }

                String usevar = col_usevar->getDataAt(row_num).toString();

                Float64 se = std::sqrt(props_x * (1.0 - props_x) / trials_x + props_y * (1.0 - props_y) / trials_y);

                /// z-statistics
                /// z = \frac{ \bar{p_{1}} - \bar{p_{2}} }{ \sqrt{ \frac{ \bar{p_{1}} \left ( 1 - \bar{p_{1}} \right ) }{ n_{1} } \frac{ \bar{p_{2}} \left ( 1 - \bar{p_{2}} \right ) }{ n_{2} } } }
                Float64 zstat;
                if (usevar == UNPOOLED)
                {
                    zstat = (props_x - props_y) / se;
                }
                else if (usevar == POOLED)
                {
                    UInt64 successes_total = successes_x + successes_y;
                    Float64 p_pooled = static_cast<Float64>(successes_total) / trials_total;
                    Float64 trials_fact = 1.0 / trials_x + 1.0 / trials_y;
                    zstat = diff / std::sqrt(p_pooled * (1.0 - p_pooled) * trials_fact);
                }
                else
                {
                    nan(to);
                    continue;
                }

                if (!std::isfinite(zstat))
                {
                    nan(to);
                    continue;
                }

                // pvalue
                boost::math::normal_distribution<> nd(0.0, 1.0);
                Float64 pvalue = 0;
                Float64 one_side = 1 - boost::math::cdf(nd, std::abs(zstat));
                pvalue = one_side * 2;

                // Confidence intervals
                Float64 d = props_x - props_y;
                Float64 z = -boost::math::quantile(nd, (1.0 - confidence_level) / 2.0);
                Float64 dist = z * se;
                Float64 ci_low = d - dist;
                Float64 ci_high = d + dist;

                Tuple tuple({zstat, pvalue, ci_low, ci_high});
                to->insert(tuple);
            }

            return to;
        }

    };


    void registerFunctionZTest(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionTwoSampleProportionsZTest>();
    }

}
