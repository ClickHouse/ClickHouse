#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <boost/math/distributions/normal.hpp>
#include <cmath>
#include <cfloat>


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

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionMinSampleSize<Impl>>();
        }

        String getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return Impl::num_args; }

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
            };

            Strings names
            {
                "minimum_sample_size",
                "detect_range_lower",
                "detect_range_upper",
            };

            return std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            );
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


    struct ContinousImpl
    {
        static constexpr auto name = "minSampleSizeContinous";
        static constexpr size_t num_args = 5;

        static void validateArguments(const DataTypes & arguments)
        {
            for (size_t i = 0; i < 1; ++i)
            {
                if (!isNativeNumber(arguments[i]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a native number.", i + 1, name);
                }
            }

            for (size_t i = 2; i < arguments.size(); ++i)
            {
                if (!isFloat(arguments[i]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a float.", i + 1, name);
                }
            }
        }

        static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
        {
            const IColumn & col_baseline = *arguments[0].column;
            const IColumn & col_sigma = *arguments[1].column;
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

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                /// Mean of control-metric
                Float64 baseline = col_baseline.getFloat64(row_num);
                /// Standard deviation of conrol-metric
                Float64 sigma = col_sigma.getFloat64(row_num);
                /// Minimal Detectable Effect
                Float64 mde = col_mde.getFloat64(row_num);
                /// Sufficient statistical power to detect a treatment effect
                Float64 power = col_power.getFloat64(row_num);
                /// Significance level
                Float64 alpha = col_alpha.getFloat64(row_num);

                if (!std::isfinite(baseline) || !std::isfinite(sigma) || !isBetweenZeroAndOne(mde) || !isBetweenZeroAndOne(power) || !isBetweenZeroAndOne(alpha))
                {
                    data_min_sample_size.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                    data_detect_lower.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                    data_detect_upper.emplace_back(std::numeric_limits<Float64>::quiet_NaN());
                    continue;
                }

                Float64 delta = baseline * mde;

                using namespace boost::math;
                normal_distribution<> nd(0.0, 1.0);
                Float64 min_sample_size = 2 * (std::pow(sigma, 2)) * std::pow(quantile(nd, 1.0 - alpha / 2) + quantile(nd, power), 2) / std::pow(delta, 2);

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

        static void validateArguments(const DataTypes & arguments)
        {
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                if (!isFloat(arguments[i]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a float.", i + 1, name);
                }
            }
        }

        static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
        {
            const IColumn & col_p1 = *arguments[0].column;
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

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                /// Mean of control-metric
                Float64 p1 = col_p1.getFloat64(row_num);
                /// Minimal Detectable Effect
                Float64 mde = col_mde.getFloat64(row_num);
                /// Sufficient statistical power to detect a treatment effect
                Float64 power = col_power.getFloat64(row_num);
                /// Significance level
                Float64 alpha = col_alpha.getFloat64(row_num);

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

                using namespace boost::math;
                normal_distribution<> nd(0.0, 1.0);
                Float64 min_sample_size = std::pow(
                    quantile(nd, 1.0 - alpha / 2.0) * std::sqrt(2.0 * p_bar * (1 - p_bar))
                    + quantile(nd, power) * std::sqrt(p1 * q1 + p2 * q2), 2
                ) / std::pow(mde, 2);

                data_min_sample_size.emplace_back(min_sample_size);
                data_detect_lower.emplace_back(p1 - mde);
                data_detect_upper.emplace_back(p1 + mde);
            }

            return ColumnTuple::create(Columns{std::move(res_min_sample_size), std::move(res_detect_lower), std::move(res_detect_upper)});
        }
    };


    void registerFunctionMinSampleSize(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionMinSampleSize<ContinousImpl>>();
        factory.registerFunction<FunctionMinSampleSize<ConversionImpl>>();
    }

}

