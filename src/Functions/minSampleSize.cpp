#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/castTypeToEither.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Common/typeid_cast.h>
#include <Interpreters/castColumn.h>
#include <boost/math/distributions/normal.hpp>
#include <cmath>
#include <cfloat>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }

    static boost::math::normal_distribution<> nd(0.0, 1.0);

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
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
        {
            return ColumnNumbers(std::begin(Impl::const_args), std::end(Impl::const_args));
        }

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


    enum class OpCase { Vector, ConstBaseline, ConstSigma, ConstBoth };

    template <typename DataType> constexpr bool IsFloatingPoint = false;
    template <> inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
    template <> inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;


    struct ContinousImpl
    {
        static constexpr auto name = "minSampleSizeContinous";
        static constexpr size_t num_args = 5;
        static constexpr size_t const_args[] = {3,4,5};

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
                if (!isNumber(arguments[i]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a float.", i + 1, name);
                }
            }
        }

        static bool castType(const IDataType * type, auto && f)
        {
            using Types = TypeList<
                DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;

            using Floats = TypeList<DataTypeFloat32, DataTypeFloat64>;

            using ValidTypes = TypeListConcat<Types, Floats>;

            return castTypeToEither(ValidTypes{}, type, std::forward<decltype(f)>(f));
        }

        template <typename F>
        static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
        {
            return castType(left, [&](const auto & left_)
            {
                return castType(right, [&](const auto & right_)
                {
                    return f(left_, right_);
                });
            });
        }

        template <typename A, typename B>
        static ColumnPtr executeNumeric(const ColumnsWithTypeAndName & arguments, const A & left, const B & right, const size_t input_rows_count)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ColVecT0 = ColumnVector<T0>;
            using ColVecT1 = ColumnVector<T1>;

            ColumnPtr left_col = nullptr;
            ColumnPtr right_col = nullptr;

            left_col = arguments[0].column;
            right_col = arguments[1].column;

            const auto * const col_left_raw = left_col.get();
            const auto * const col_right_raw = right_col.get();
            const ColumnConst * const col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw);
            const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw);
            const ColVecT0 * const col_left = checkAndGetColumn<ColVecT0>(col_left_raw);
            const ColVecT1 * const col_right = checkAndGetColumn<ColVecT1>(col_right_raw);

            if (col_left && col_right)
            {
                return process<OpCase::Vector>(arguments, col_left->getData().data(), col_right->getData().data(), input_rows_count);
            }
            else if (col_left_const && col_right_const)
            {
                const T0 left_value = col_left_const->template getValue<T0>();
                const T1 right_value = col_right_const->template getValue<T1>();
                return process<OpCase::ConstBoth>(arguments, &left_value, &right_value, input_rows_count);
            }
            else if (col_left_const && col_right)
            {
                const T0 value = col_left_const->template getValue<T0>();
                return process<OpCase::ConstBaseline>(arguments, &value, col_right->getData().data(), input_rows_count);
            }
            else if (col_left && col_right_const)
            {
                const T1 value = col_right_const->template getValue<T1>();
                return process<OpCase::ConstSigma>(arguments, col_left->getData().data(), &value, input_rows_count);
            }
            else
            {
                return nullptr;
            }
        }

        template <OpCase op_case, typename T0, typename T1>
        static ColumnPtr process(const ColumnsWithTypeAndName & arguments, const T0 * col_baseline, const T1 * col_sigma, const size_t input_rows_count)
        {
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

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                /// Mean of control-metric
                Float64 baseline;
                /// Standard deviation of conrol-metric
                Float64 sigma;

                if constexpr (op_case == OpCase::ConstBaseline)
                {
                    baseline = static_cast<Float64>(col_baseline[0]);
                    sigma = static_cast<Float64>(col_sigma[row_num]);
                }
                else if constexpr (op_case == OpCase::ConstSigma)
                {
                    baseline = static_cast<Float64>(col_baseline[row_num]);
                    sigma = static_cast<Float64>(col_sigma[0]);
                }
                else if constexpr (op_case == OpCase::Vector)
                {
                    baseline = static_cast<Float64>(col_baseline[row_num]);
                    sigma = static_cast<Float64>(col_sigma[row_num]);
                }
                else if constexpr (op_case == OpCase::ConstBoth)
                {
                    baseline = static_cast<Float64>(col_baseline[0]);
                    sigma = static_cast<Float64>(col_sigma[0]);
                }

                if (!std::isfinite(baseline) || !std::isfinite(sigma) || !isBetweenZeroAndOne(mde) || !isBetweenZeroAndOne(power) || !isBetweenZeroAndOne(alpha))
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
                Float64 min_sample_size = 2 * std::pow(sigma, 2) * std::pow(quantile(nd, 1.0 - alpha / 2) + quantile(nd, power), 2) / std::pow(delta, 2);

                data_min_sample_size.emplace_back(min_sample_size);
                data_detect_lower.emplace_back(baseline - delta);
                data_detect_upper.emplace_back(baseline + delta);
            }

            return ColumnTuple::create(Columns{std::move(res_min_sample_size), std::move(res_detect_lower), std::move(res_detect_upper)});
        }

        static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
        {
            const auto & arg_baseline = arguments[0];
            const auto & arg_sigma = arguments[1];
            ColumnPtr res;

            /// The first and the second arguments are both unsigned integers or floats so cast both to Float64.
            const bool valid = castBothTypes(arg_baseline.type.get(), arg_sigma.type.get(), [&](const auto & left, const auto & right)
            {
                return (res = executeNumeric(arguments, left, right, input_rows_count)) != nullptr;
            });

            if (!valid)
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of '{}' have unsupported data types: '{}' of type '{}', '{}' of type '{}'", name,
                        arg_baseline.name, arg_baseline.type->getName(), arg_sigma.name, arg_sigma.type->getName());
            }
            return res;
        }
    };


    struct ConversionImpl
    {
        static constexpr auto name = "minSampleSizeConversion";
        static constexpr size_t num_args = 4;
        static constexpr size_t const_args[] = {2,3,4};

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
            if (const ColumnConst * const col_p1_const = checkAndGetColumnConst<ColumnVector<Float64>>(arguments[0].column.get()))
            {
                const Float64 left_value = col_p1_const->template getValue<Float64>();
                return process<true>(arguments, &left_value, input_rows_count);
            }
            else if (const ColumnVector<Float64> * const col_p1 = checkAndGetColumn<ColumnVector<Float64>>(arguments[0].column.get()))
            {
                return process<false>(arguments, col_p1->getData().data(), input_rows_count);
            }
            else
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must be a float.", name);
            }
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
                Float64 min_sample_size = std::pow(
                    quantile(nd, 1.0 - alpha / 2.0) * std::sqrt(2.0 * p_bar * q_bar)
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

