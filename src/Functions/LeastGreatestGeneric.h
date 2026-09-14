#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Core/Settings.h>
#include <Functions/castTypeToEither.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool least_greatest_legacy_null_behavior;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


enum class LeastGreatest : uint8_t
{
    Least,
    Greatest
};


template <LeastGreatest kind>
class FunctionLeastGreatestGeneric final : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionLeastGreatestGeneric<kind>>(context); }

    /// TODO Remove support for legacy NULL behavior (can be done end of 2026)

    explicit FunctionLeastGreatestGeneric(ContextPtr context)
        : legacy_null_behavior(context->getSettingsRef()[Setting::least_greatest_legacy_null_behavior])
    {
    }

private:
    /// A constant argument is kept as its single data row instead of being materialized.
    struct ConvertedColumn
    {
        ColumnPtr column = nullptr;
        bool is_const = false;
    };

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return legacy_null_behavior; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        return getLeastSupertype(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() == 1)
            return arguments[0].column;

        VectorWithMemoryTracking<ConvertedColumn> converted_columns;
        converted_columns.reserve(arguments.size());
        bool has_const_column = false;
        for (const auto & argument : arguments)
        {
            if (!legacy_null_behavior && argument.type->onlyNull())
                continue; /// ignore NULL arguments

            auto converted_col = castColumn(argument, result_type);
            if (const auto * const_column = checkAndGetColumn<ColumnConst>(converted_col.get()))
            {
                converted_columns.push_back({const_column->getDataColumnPtr(), true});
                has_const_column = true;
            }
            else
            {
                converted_columns.push_back({converted_col->convertToFullColumnIfConst(), false});
            }
        }

        if (!legacy_null_behavior && converted_columns.empty())
            return arguments[0].column;
        else if (!legacy_null_behavior && converted_columns.size() == 1)
        {
            if (converted_columns[0].is_const)
                return ColumnConst::create(converted_columns[0].column, input_rows_count);
            return converted_columns[0].column;
        }

        if (ColumnPtr res = executeNumeric(converted_columns, result_type, input_rows_count))
            return res;

        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);

        if (!has_const_column)
        {
            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                size_t best_arg = 0;
                for (size_t arg = 1; arg < converted_columns.size(); ++arg)
                {
                    if constexpr (kind == LeastGreatest::Least)
                    {
                        auto cmp_result = converted_columns[arg].column->compareAt(
                            row_num, row_num, *converted_columns[best_arg].column, 1);
                        if (cmp_result < 0)
                            best_arg = arg;
                    }
                    else
                    {
                        auto cmp_result = converted_columns[arg].column->compareAt(
                            row_num, row_num, *converted_columns[best_arg].column, -1);
                        if (cmp_result > 0)
                            best_arg = arg;
                    }
                }

                result_column->insertFrom(*converted_columns[best_arg].column, row_num);
            }

            return result_column;
        }

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t best_arg = 0;
            size_t best_row = converted_columns[0].is_const ? 0 : row_num;
            for (size_t arg = 1; arg < converted_columns.size(); ++arg)
            {
                const auto & current = converted_columns[arg];
                const size_t current_row = current.is_const ? 0 : row_num;

                if constexpr (kind == LeastGreatest::Least)
                {
                    auto cmp_result = current.column->compareAt(current_row, best_row, *converted_columns[best_arg].column, 1);
                    if (cmp_result < 0)
                    {
                        best_arg = arg;
                        best_row = current_row;
                    }
                }
                else
                {
                    auto cmp_result = current.column->compareAt(current_row, best_row, *converted_columns[best_arg].column, -1);
                    if (cmp_result > 0)
                    {
                        best_arg = arg;
                        best_row = current_row;
                    }
                }
            }

            result_column->insertFrom(*converted_columns[best_arg].column, best_row);
        }

        return result_column;
    }

    /// Replicates `compareAt` with nan_direction_hint = 1 for least / -1 for greatest:
    /// NaN loses against any number, and on ties the earlier argument is kept.
    template <typename T>
    static bool takesPrecedence(T current, T incoming)
    {
        if constexpr (is_floating_point<T>)
        {
            if (isNaN(incoming))
                return false;
            if (isNaN(current))
                return true;
        }
        return kind == LeastGreatest::Least ? incoming < current : incoming > current;
    }

    /// Vectorized min/max fold for numeric (possibly Nullable) arguments of any arity.
    /// NULL loses against any value, so the result is NULL only where all arguments are NULL -
    /// same as the per-row loop above with null_direction_hint = 1 for least / -1 for greatest.
    /// Returns nullptr if the result type is not a (possibly Nullable) number.
    static ColumnPtr executeNumeric(const VectorWithMemoryTracking<ConvertedColumn> & columns, const DataTypePtr & result_type, size_t input_rows_count)
    {
        ColumnPtr res;
        castTypeToEither<
            DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
            DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
            DataTypeBFloat16, DataTypeFloat32, DataTypeFloat64>(
            removeNullable(result_type).get(),
            [&](const auto & type)
            {
                using T = typename std::decay_t<decltype(type)>::FieldType;
                res = result_type->isNullable()
                    ? executeNumericImpl<T, true>(columns, input_rows_count)
                    : executeNumericImpl<T, false>(columns, input_rows_count);
                return res != nullptr;
            });
        return res;
    }

    template <typename T, bool is_nullable>
    static ColumnPtr executeNumericImpl(const VectorWithMemoryTracking<ConvertedColumn> & columns, size_t input_rows_count)
    {
        chassert(columns.size() >= 2);

        auto res_nested = ColumnVector<T>::create();
        ColumnUInt8::MutablePtr res_null_map;
        if constexpr (is_nullable)
            res_null_map = ColumnUInt8::create();

        /// The first column is not copied into the accumulator: the first fold reads it
        /// directly and writes the accumulator, saving a full pass over the data.
        const T * first_data = nullptr;
        const UInt8 * first_null = nullptr;
        bool first_is_const = false;

        for (size_t arg = 0; arg < columns.size(); ++arg)
        {
            const IColumn * column = columns[arg].column.get();
            const bool b_is_const = columns[arg].is_const;
            const UInt8 * b_null = nullptr;
            if constexpr (is_nullable)
            {
                const auto * column_nullable = checkAndGetColumn<ColumnNullable>(column);
                if (!column_nullable)
                    return nullptr;
                b_null = column_nullable->getNullMapData().data();
                column = &column_nullable->getNestedColumn();
            }
            const auto * column_vector = checkAndGetColumn<ColumnVector<T>>(column);
            if (!column_vector)
                return nullptr;
            const T * b = column_vector->getData().data();

            if (arg == 0)
            {
                first_data = b;
                first_null = b_null;
                first_is_const = b_is_const;
                continue;
            }
            if (arg == 1)
            {
                res_nested->getData().resize(input_rows_count);
                if constexpr (is_nullable)
                    res_null_map->getData().resize(input_rows_count);
            }

            T * a = res_nested->getData().data();
            UInt8 * a_null = nullptr;
            if constexpr (is_nullable)
                a_null = res_null_map->getData().data();
            const T * x = arg == 1 ? first_data : a;
            const UInt8 * x_null = arg == 1 ? first_null : a_null;
            const bool x_is_const = arg == 1 && first_is_const;

            /// A constant side has a single row, so it is read at index 0 on every iteration.
            auto fold = [&]<bool x_const, bool b_const>()
            {
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const size_t xi = x_const ? 0 : i;
                    const size_t bi = b_const ? 0 : i;
                    if constexpr (is_nullable)
                    {
                        /// NULL loses against any value, so only two non-NULL sides are actually compared.
                        if (x_null[xi])
                            a[i] = b[bi];
                        else if (b_null[bi])
                            a[i] = x[xi];
                        else
                            a[i] = takesPrecedence(x[xi], b[bi]) ? b[bi] : x[xi];
                        a_null[i] = x_null[xi] && b_null[bi];
                    }
                    else
                    {
                        a[i] = takesPrecedence(x[xi], b[bi]) ? b[bi] : x[xi];
                    }
                }
            };

            if (x_is_const && b_is_const)
                fold.template operator()<true, true>();
            else if (x_is_const)
                fold.template operator()<true, false>();
            else if (b_is_const)
                fold.template operator()<false, true>();
            else
                fold.template operator()<false, false>();
        }

        if constexpr (is_nullable)
            return ColumnNullable::create(std::move(res_nested), std::move(res_null_map));
        else
            return res_nested;
    }

    bool legacy_null_behavior;
};

template <LeastGreatest kind, typename SpecializedFunction>
class LeastGreatestOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";
    static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<LeastGreatestOverloadResolver<kind, SpecializedFunction>>(context_); }

    explicit LeastGreatestOverloadResolver(ContextPtr context_)
        : context(context_)
        , legacy_null_behavior(context_->getSettingsRef()[Setting::least_greatest_legacy_null_behavior])
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return legacy_null_behavior; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2)
        {
            auto arg_0_type = legacy_null_behavior ? removeNullable(arguments[0].type) : arguments[0].type;
            auto arg_1_type = legacy_null_behavior ? removeNullable(arguments[1].type) : arguments[1].type;
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return std::make_unique<FunctionToFunctionBaseAdaptor>(SpecializedFunction::create(context), argument_types, return_type);
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionLeastGreatestGeneric<kind>::create(context), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        if (types.size() == 2)
        {
            auto arg_0_type = legacy_null_behavior ? removeNullable(types[0]) : types[0];
            auto arg_1_type = legacy_null_behavior ? removeNullable(types[1]) : types[1];
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return SpecializedFunction::create(context)->getReturnTypeImpl(types);
        }

        return getLeastSupertype(types);
    }

private:
    ContextPtr context;
    bool legacy_null_behavior;
};

}
