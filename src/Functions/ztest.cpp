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
        extern const int ILLEGAL_COLUMN;
    }


    class FunctionTwoSampleProportionsZTest : public IFunction
    {
    public:
        static constexpr auto TWO_SIDED = "two-sided";
        static constexpr auto LARGER = "larger";
        static constexpr auto SMALLER  = "smaller";
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

        size_t getNumberOfArguments() const override { return 7; }

        bool useDefaultImplementationForNulls() const override { return false; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnType() const
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

        const ColumnString * getColumnString(const ColumnWithTypeAndName & argument) const
        {
            const auto & first_column = argument;
            if (!isString(first_column.type))
                throw Exception{"The fifth and sixth argument of function " + getName() + " should be strings, illegal type: " + first_column.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            const ColumnPtr & arg_json = first_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_string)
                throw Exception{"Illegal column " + arg_json->getName(), ErrorCodes::ILLEGAL_COLUMN};

            return col_json_string;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            MutableColumnPtr to{getReturnType()->createColumn()};
            to->reserve(input_rows_count);

            auto * col_alternative = getColumnString(arguments[5]);
            const ColumnString::Chars & alternative_chars = col_alternative->getChars();
            const ColumnString::Offsets & alternative_offsets = col_alternative->getOffsets();

            auto * col_usevar = getColumnString(arguments[6]);
            const ColumnString::Chars & usevar_chars = col_usevar->getChars();
            const ColumnString::Offsets & usevar_offsets = col_usevar->getOffsets();

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                const UInt64 successes_x = arguments[0].column->getUInt(row_num);
                const UInt64 successes_y = arguments[1].column->getUInt(row_num);
                const UInt64 trials_x = arguments[2].column->getUInt(row_num);
                const UInt64 trials_y = arguments[3].column->getUInt(row_num);
                const Float64 confidence_interval = arguments[4].column->getFloat64(row_num);

                std::string_view alternative{reinterpret_cast<const char *>(&alternative_chars[alternative_offsets[row_num - 1]]), alternative_offsets[row_num] - alternative_offsets[row_num - 1] - 1};
                std::string_view usevar{reinterpret_cast<const char *>(&usevar_chars[usevar_offsets[row_num - 1]]), usevar_offsets[row_num] - usevar_offsets[row_num - 1] - 1};

                const Float64 props_x = static_cast<Float64>(successes_x) / trials_x;
                const Float64 props_y = static_cast<Float64>(successes_y) / trials_y;
                const Float64 diff = props_x - props_y;

                const UInt64 trials_total = trials_x + trials_y;
                if (trials_total == 0)
                {
                    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
                    Tuple tuple({nan, nan, nan, nan});
                    to->insert(tuple);
                    continue;
                }

                Float64 se = std::sqrt(props_x * (1.0 - props_x) / trials_x + props_y * (1.0 - props_y) / trials_y);

                // z-statistics
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
                    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
                    Tuple tuple({nan, nan, nan, nan});
                    to->insert(tuple);
                    continue;
                }

                // pvalue
                boost::math::normal_distribution<> nd(0.0, 1.0);
                Float64 pvalue = 0;
                if (alternative == TWO_SIDED)
                {
                    Float64 one_side = 1 - boost::math::cdf(nd, std::abs(zstat));
                    pvalue = one_side * 2;
                }
                else if (alternative == LARGER)
                {
                    pvalue = 1.0 - boost::math::cdf(nd, std::abs(zstat));
                }
                else if (alternative == SMALLER)
                {
                    pvalue = boost::math::cdf(nd, std::abs(zstat));
                }
                else
                {
                    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
                    Tuple tuple({nan, nan, nan, nan});
                    to->insert(tuple);
                    continue;
                }

                // Confidence intervals
                Float64 d = props_x - props_y;
                Float64 z = -boost::math::quantile(nd, (1.0 - confidence_interval) / 2.0);
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
