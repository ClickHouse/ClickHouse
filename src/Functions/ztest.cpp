#include <Common/typeid_cast.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/castTypeToEither.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>
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
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {5, 6}; }

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

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {

            for (size_t i = 0; i < 4; ++i)
            {
                if (!isNativeInteger(arguments[i]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The {}th Argument of function {} must be a interger or unsigned integer.", i + 1, getName());
                }
            }

            if (!isFloat(arguments[4]))
            {
                throw Exception{"The fifth argument of function " + getName() + " should be a float, illegal type: " + arguments[4]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }

            if (!isString(arguments[5]))
            {
                throw Exception{"The sixth argument of function " + getName() + " should be a string, illegal type: " + arguments[5]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }

            return getReturnType();
        }

        static bool castType(const IDataType * type, auto && f)
        {
            using ValidTypes = TypeList<
                DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;

            return castTypeToEither(ValidTypes{}, type, std::forward<decltype(f)>(f));
        }

        template <typename F>
        static bool castTypes(const IDataType * first, const IDataType * second, const IDataType * third, const IDataType * fourth, F && f)
        {
            return castType(first, [&](const auto & first_)
            {
                return castType(second, [&](const auto & second_)
                {
                    return castType(third, [&](const auto & third_)
                    {
                        return castType(fourth, [&](const auto & fourth_)
                        {
                            return f(first_, second_, third_, fourth_);
                        });
                    });
                });
            });
        }

        static void nan(MutableColumnPtr & to)
        {
            Tuple tuple({nan, nan, nan, nan});
            to->insert(tuple);
        }

        template <typename A, typename B, typename C, typename D, bool const_a, bool const_b, bool const_c, bool const_d>
        static ColumnPtr calculate(const ColumnsWithTypeAndName & arguments, const A * col_sx, const B * col_sy, const C * col_tx, const D * col_ty, const size_t input_rows_count)
        {
            #define FILL_NAN const Float64 nan = std::numeric_limits<Float64>::quiet_NaN(); \
                                                 data_z_statistic.emplace_back(nan); \
                                                 data_p_value.emplace_back(nan); \
                                                 data_ci_lower.emplace_back(nan); \
                                                 data_ci_upper.emplace_back(nan);

            UInt64 successes_x;
            UInt64 successes_y;
            UInt64 trials_x;
            UInt64 trials_y;

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

            const ColumnPtr & col_confidence_level = arguments[4].column;
            Float64 confidence_level = col_confidence_level->getFloat64(0);
            if (!std::isfinite(confidence_level) || confidence_level < 0.0 || confidence_level > 1.0)
            {
                FILL_NAN
                return ColumnTuple::create(Columns{std::move(res_z_statistic), std::move(res_p_value), std::move(res_ci_lower), std::move(res_ci_upper)});
            }

            const ColumnPtr & col_usevar = arguments[5].column;
            String usevar = col_usevar->getDataAt(0).toString();

            boost::math::normal_distribution<> nd(0.0, 1.0);

            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                if constexpr (const_a)
                    successes_x = static_cast<UInt64>(col_sx[0]);
                else
                    successes_x = static_cast<UInt64>(col_sx[row_num]);

                if constexpr (const_b)
                    successes_y = static_cast<UInt64>(col_sy[0]);
                else
                    successes_y = static_cast<UInt64>(col_sy[row_num]);

                if constexpr (const_c)
                    trials_x = static_cast<UInt64>(col_tx[0]);
                else
                    trials_x = static_cast<UInt64>(col_tx[row_num]);

                if constexpr (const_d)
                    trials_y = static_cast<UInt64>(col_ty[0]);
                else
                    trials_y = static_cast<UInt64>(col_ty[row_num]);

                const Float64 props_x = static_cast<Float64>(successes_x) / trials_x;
                const Float64 props_y = static_cast<Float64>(successes_y) / trials_y;
                const Float64 diff = props_x - props_y;
                const UInt64 trials_total = trials_x + trials_y;

                if (successes_x == 0 || successes_y == 0
                    || successes_x > trials_x || successes_y > trials_y
                    || trials_total == 0)
                {
                    FILL_NAN
                    continue;
                }

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
                    FILL_NAN
                    continue;
                }

                if (!std::isfinite(zstat))
                {
                    FILL_NAN
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

                data_z_statistic.emplace_back(zstat);
                data_p_value.emplace_back(pvalue);
                data_ci_lower.emplace_back(ci_low);
                data_ci_upper.emplace_back(ci_high);
            }

            return ColumnTuple::create(Columns{std::move(res_z_statistic), std::move(res_p_value), std::move(res_ci_lower), std::move(res_ci_upper)});
        }

        template <typename A, typename B, typename C, typename D>
        static ColumnPtr execute([[maybe_unused]] const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const A & sx, [[maybe_unused]] const B & sy, [[maybe_unused]] const C & tx, [[maybe_unused]] const D & ty, [[maybe_unused]] const size_t input_rows_count)
        {
            using SXDataType = std::decay_t<decltype(sx)>;
            using SYDataType = std::decay_t<decltype(sy)>;
            using TXDataType = std::decay_t<decltype(tx)>;
            using TYDataType = std::decay_t<decltype(ty)>;

            using TSX = typename SXDataType::FieldType;
            using TSY = typename SYDataType::FieldType;
            using TTX = typename TXDataType::FieldType;
            using TTY = typename TYDataType::FieldType;

            using ColVecTSX = ColumnVector<TSX>;
            using ColVecTSY = ColumnVector<TSY>;
            using ColVecTTX = ColumnVector<TTX>;
            using ColVecTTY = ColumnVector<TTY>;

            ColumnPtr sx_col = nullptr;
            ColumnPtr sy_col = nullptr;
            ColumnPtr tx_col = nullptr;
            ColumnPtr ty_col = nullptr;

            sx_col = arguments[0].column;
            sy_col = arguments[1].column;
            tx_col = arguments[2].column;
            ty_col = arguments[3].column;

            const auto * const col_sx_raw = sx_col.get();
            const auto * const col_sy_raw = sy_col.get();
            const auto * const col_tx_raw = tx_col.get();
            const auto * const col_ty_raw = ty_col.get();

            const ColumnConst * const col_sx_const = checkAndGetColumnConst<ColVecTSX>(col_sx_raw);
            const ColumnConst * const col_sy_const = checkAndGetColumnConst<ColVecTSY>(col_sy_raw);
            const ColumnConst * const col_tx_const = checkAndGetColumnConst<ColVecTTX>(col_tx_raw);
            const ColumnConst * const col_ty_const = checkAndGetColumnConst<ColVecTTY>(col_ty_raw);

            const ColVecTSX * const col_sx = checkAndGetColumn<ColVecTSX>(col_sx_raw);
            const ColVecTSY * const col_sy = checkAndGetColumn<ColVecTSY>(col_sy_raw);
            const ColVecTTX * const col_tx = checkAndGetColumn<ColVecTTX>(col_tx_raw);
            const ColVecTTY * const col_ty = checkAndGetColumn<ColVecTTY>(col_ty_raw);


            if (col_sx && col_sy && col_tx && col_ty)
            {
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), col_sy->getData().data(), col_tx->getData().data(), col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx && col_sy && col_tx && col_ty_const)
            {
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), col_sy->getData().data(), col_tx->getData().data(), &ty_value, input_rows_count);
            }
            else if (col_sx && col_sy && col_tx_const && col_ty)
            {
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), col_sy->getData().data(), &tx_value, col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx && col_sy && col_tx_const && col_ty_const)
            {
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), col_sy->getData().data(), &tx_value, &ty_value, input_rows_count);
            }
            else if (col_sx && col_sy_const && col_tx && col_ty)
            {
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), &sy_value, col_tx->getData().data(), col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx && col_sy_const && col_tx && col_ty_const)
            {
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), &sy_value, col_tx->getData().data(), &ty_value, input_rows_count);
            }
            else if (col_sx && col_sy_const && col_tx_const && col_ty)
            {
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), &sy_value, &tx_value, col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx && col_sy_const && col_tx_const && col_ty_const)
            {
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, col_sx->getData().data(), &sy_value, &tx_value, &ty_value, input_rows_count);
            }
            else if (col_sx_const && col_sy && col_tx && col_ty)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, col_sy->getData().data(), col_tx->getData().data(), col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx_const && col_sy && col_tx && col_ty_const)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, col_sy->getData().data(), col_tx->getData().data(), &ty_value, input_rows_count);
            }
            else if (col_sx_const && col_sy && col_tx_const && col_ty)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, col_sy->getData().data(), &tx_value, col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx_const && col_sy && col_tx_const && col_ty_const)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, col_sy->getData().data(), &tx_value, &ty_value, input_rows_count);
            }
            else if (col_sx_const && col_sy_const && col_tx && col_ty)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, &sy_value, col_tx->getData().data(), col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx_const && col_sy_const && col_tx && col_ty_const)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, &sy_value, col_tx->getData().data(), &ty_value, input_rows_count);
            }
            else if (col_sx_const && col_sy_const && col_tx_const && col_ty)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, &sy_value, &tx_value, col_ty->getData().data(), input_rows_count);
            }
            else if (col_sx_const && col_sy_const && col_tx_const && col_ty_const)
            {
                const TSX sx_value = col_sx_const->template getValue<TSX>();
                const TSY sy_value = col_sy_const->template getValue<TSY>();
                const TTX tx_value = col_tx_const->template getValue<TTX>();
                const TTY ty_value = col_ty_const->template getValue<TTY>();
                return calculate<TSX, TSY, TTX, TTY, false, false, false, false>(arguments, &sx_value, &sy_value, &tx_value, &ty_value, input_rows_count);
            }
            else
            {
                return nullptr;
            }
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            ColumnPtr res;

            const bool valid = castTypes(arguments[0].type.get(), arguments[1].type.get(), arguments[2].type.get(), arguments[3].type.get(),
                                         [&]([[maybe_unused]] const auto & first, [[maybe_unused]] const auto & second, [[maybe_unused]] const auto & third, [[maybe_unused]] const auto & fourth)
            {
                return (res = execute(arguments, first, second, third, fourth, input_rows_count)) != nullptr;
            });

            if (!valid)
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Arguments of '{}' have unsupported data types: '{}' of type '{}', '{}' of type '{}', '{}' of type '{}', '{}' of type '{}'",
                        name, arguments[0].name, arguments[0].type->getName(), arguments[1].name, arguments[1].type->getName(),
                        arguments[2].name, arguments[2].type->getName(), arguments[3].name, arguments[3].type->getName());
            }
            return res;
        }

    };


    void registerFunctionZTest(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionTwoSampleProportionsZTest>();
    }

}
