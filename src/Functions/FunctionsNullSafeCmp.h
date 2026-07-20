#pragma once
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsComparison.h>
#include <Common/quoteString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

enum class NullSafeCmpMode : uint8_t
{
    NullSafeEqual,
    NullSafeNotEqual
};

template <
    typename Name,                                              // Function Name
    NullSafeCmpMode cmp_mode,                                   // Null-safe mode (Equal or NotEqual)
    template <typename, typename > class CompareOp,             // EqualsOp / NotEqualsOp
    typename CompareName>                                       // NameEquals / NameNotEquals
class FunctionsNullSafeCmp final : public IFunction
{
private:
    const ComparisonParams params;

    static bool containsNothing(const DataTypePtr & type)
    {
        if (isNothing(type))
            return true;

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            for (const auto & elem : tuple_type->getElements())
            {
                if (containsNothing(elem))
                    return true;
            }
        }
        return false;
    }

public:
    explicit FunctionsNullSafeCmp(ComparisonParams params_) : params(std::move(params_)) {}

    static constexpr auto name = Name::name;
    static constexpr bool is_equal_mode = (cmp_mode == NullSafeCmpMode::NullSafeEqual);


    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionsNullSafeCmp>(context ? ComparisonParams(context) : ComparisonParams());
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} expects exactly 2 arguments, got {}",
                            backQuote(name),
                            arguments.size());

        const DataTypePtr & left_ele_type = arguments[0];
        const DataTypePtr & right_ele_type = arguments[1];

        if (containsNothing(left_ele_type) || containsNothing(right_ele_type))
            return std::make_shared<DataTypeNothing>();

        if ((isMap(left_ele_type) && right_ele_type->onlyNull())
                || (left_ele_type->onlyNull() && isMap(right_ele_type))
                || (isArray(left_ele_type) && right_ele_type->onlyNull())
                || (left_ele_type->onlyNull() && isArray(right_ele_type)))
        {
            return std::make_shared<DataTypeUInt8>();
        }

        if (!tryGetLeastSupertype(arguments))
        {
            /// A top-level `String`/`FixedString` on one side with no least common supertype can
            /// never be compared null-safely (unlike the regular `=` / `!=` operators.
            const bool has_string_type
                = WhichDataType(removeLowCardinalityAndNullable(left_ele_type)).isStringOrFixedString()
                || WhichDataType(removeLowCardinalityAndNullable(right_ele_type)).isStringOrFixedString();
            if (has_string_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal types of arguments ({}, {}) of function {}",
                    backQuote(left_ele_type->getName()),
                    backQuote(right_ele_type->getName()),
                    backQuote(name));

            /// Types like `UInt64` vs `Int64` (or arrays/tuples of them) have no least common
            /// supertype but are still comparable element-wise using accurate comparison, exactly
            /// like the regular `=` / `!=` operators.
            FunctionOverloadResolverPtr comparator = std::make_unique<FunctionToOverloadResolverAdaptor>(
                std::make_shared<FunctionComparison<CompareOp, CompareName, true /*is null safe*/>>(params));
            ColumnsWithTypeAndName probe_args{
                {nullptr, removeNullable(left_ele_type), ""}, {nullptr, removeNullable(right_ele_type), ""}};
            try
            {
                comparator->build(probe_args);
            }
            catch (Exception &)
            {
                /// Rethrow with our own name so the diagnostics match the actual query, e.g.
                /// `Array(String)` vs `Array(Int64)` reports `isDistinctFrom`
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal types of arguments ({}, {}) of function {}",
                    backQuote(left_ele_type->getName()),
                    backQuote(right_ele_type->getName()),
                    backQuote(name));
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr ALWAYS_INLINE executeForVariantOrDynamicAndNull(const ColumnWithTypeAndName & variant_or_dynamic_col) const
    {
        ColumnPtr col = variant_or_dynamic_col.column->convertToFullColumnIfConst();
        const auto & column_variant_or_dynamic =
            isVariant(variant_or_dynamic_col.type) ?
                checkAndGetColumn<ColumnVariant>(*col) :
                checkAndGetColumn<ColumnDynamic>(*col).getVariantColumn();
        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        data.resize(column_variant_or_dynamic.size());
        for (size_t i = 0; i < column_variant_or_dynamic.size(); ++i)
        {
            bool is_null = column_variant_or_dynamic.isNullAt(i);
            data[i] = is_equal_mode ? is_null : !is_null;
        }
        return res;
    }

    /// Null-safe comparison for types that have no least common supertype (e.g.
    /// `Nullable(UInt64)` vs `Nullable(Int64)`).
    ColumnPtr executeNullableWithoutSupertype(const ColumnsWithTypeAndName & columns_with_type_and_name, size_t input_rows_count) const
    {
        auto extract_nested_column_info = [](const ColumnWithTypeAndName & arg, ColumnPtr & null_map, ColumnWithTypeAndName & nested)
        {
            ColumnPtr full_column = arg.column->convertToFullColumnIfConst();
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(full_column.get()))
            {
                null_map = nullable->getNullMapColumnPtr();
                nested.column = nullable->getNestedColumnPtr();
                nested.type = removeNullable(arg.type);
            }
            else
            {
                null_map = nullptr;
                nested.column = full_column;
                nested.type = arg.type;
            }
        };

        ColumnPtr left_null_map;
        ColumnPtr right_null_map;
        ColumnWithTypeAndName left_nested;
        ColumnWithTypeAndName right_nested;
        extract_nested_column_info(columns_with_type_and_name[0], left_null_map, left_nested);
        extract_nested_column_info(columns_with_type_and_name[1], right_null_map, right_nested);

        /// Accurate value comparison of the non-Nullable nested columns (always yields UInt8).
        FunctionOverloadResolverPtr comparator = std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionComparison<CompareOp, CompareName, true /*is null safe*/>>(params));

        ColumnsWithTypeAndName nested_args{left_nested, right_nested};
        auto executable = comparator->build(nested_args);
        ColumnPtr value_cmp = executable->execute(nested_args, executable->getResultType(), input_rows_count, /* dry_run = */ false);
        value_cmp = value_cmp->convertToFullColumnIfConst();

        const auto & value_data = assert_cast<const ColumnUInt8 &>(*value_cmp).getData();
        const ColumnUInt8::Container * left_nulls
            = left_null_map ? &assert_cast<const ColumnUInt8 &>(*left_null_map).getData() : nullptr;
        const ColumnUInt8::Container * right_nulls
            = right_null_map ? &assert_cast<const ColumnUInt8 &>(*right_null_map).getData() : nullptr;

        auto res = ColumnUInt8::create(input_rows_count);
        auto & res_data = res->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const bool left_is_null = left_nulls && (*left_nulls)[i];
            const bool right_is_null = right_nulls && (*right_nulls)[i];

            if (left_is_null && right_is_null)
                res_data[i] = is_equal_mode ? 1 : 0;
            else if (left_is_null != right_is_null)
                res_data[i] = is_equal_mode ? 0 : 1;
            else
                res_data[i] = value_data[i];
        }
        return res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr left_col = arguments[0].column;
        ColumnPtr right_col = arguments[1].column;
        const ColumnWithTypeAndName & type_and_name_left_col = arguments[0];
        const ColumnWithTypeAndName & type_and_name_right_col = arguments[1];
        if (!left_col || !right_col)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} received null column: left_col={} right_col={}. "
                            "Please check the input columns.",
                            backQuote(name),
                            left_col ? "NOT NULL" : "NULL",
                            right_col ? "NOT NULL" : "NULL");
        }

        // To address:
        //   1. Map vs null or
        //   2. Array vs null
        // The results will be always set to 0 if is_equals_mode is true
        if (((isMap(type_and_name_left_col.type) || isArray(type_and_name_left_col.type))
                && type_and_name_right_col.type->onlyNull())
            || ((isMap(type_and_name_right_col.type) || isArray(type_and_name_right_col.type))
                && type_and_name_left_col.type->onlyNull()))
        {
            return result_type->createColumnConst(input_rows_count, UInt8(is_equal_mode ? 0 : 1));
        }

        // To address:
        //   1. Variant vs null
        //   2. Dynamic vs null
        if (((isVariant(type_and_name_left_col.type) || isDynamic(type_and_name_left_col.type))
                && type_and_name_right_col.type->onlyNull())
            || ((isVariant(type_and_name_right_col.type) || isDynamic(type_and_name_right_col.type))
                && type_and_name_left_col.type->onlyNull()))
        {
            return executeForVariantOrDynamicAndNull(
                isVariant(type_and_name_left_col.type) || isDynamic(type_and_name_left_col.type)
                    ? type_and_name_left_col
                    : type_and_name_right_col);
        }

        // get common type for null-safe comparison;
        DataTypePtr common_type = tryGetLeastSupertype(DataTypes{arguments[0].type, arguments[1].type});
        // handle string types compared with null
        bool has_string_type = WhichDataType(removeLowCardinalityAndNullable(arguments[0].type)).isStringOrFixedString()
                        || WhichDataType(removeLowCardinalityAndNullable(arguments[1].type)).isStringOrFixedString();
        if (common_type)
        {
            ColumnPtr c0_converted = castColumn(arguments[0], common_type);
            ColumnPtr c1_converted = castColumn(arguments[1], common_type);

            // To address: Nullable vs Nullable
            if (c0_converted->isNullable() && c1_converted->isNullable())
            {
                auto c_res = ColumnUInt8::create();
                ColumnUInt8::Container & vec_res = c_res->getData();
                vec_res.resize(arguments[0].column->size());
                c0_converted = c0_converted->convertToFullColumnIfConst();
                c1_converted = c1_converted->convertToFullColumnIfConst();

                for (size_t i = 0; i < input_rows_count ; i++)
                    vec_res[i] = c0_converted->compareAt(i, i, *c1_converted, 1) == 0 ? is_equal_mode : !is_equal_mode;

                return c_res;
            }
        }
        else if ((type_and_name_left_col.type->isNullable() || type_and_name_right_col.type->isNullable()) && !has_string_type)
        {
            // No common supertype and at least one side is Nullable (e.g. `Nullable(UInt64)` vs
            // `Nullable(Int64)`).
            return executeNullableWithoutSupertype(arguments, input_rows_count);
        }

        // To address regular case (also covers types with no common supertype that are not Nullable,
        // e.g. `UInt64` vs `Int64` and `Array(UInt64)` vs `Array(Int64)`;
        ColumnPtr res;
        FunctionOverloadResolverPtr comparator
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionComparison<CompareOp, CompareName, true /*is null safe cmp mode*/>>(params));

        auto executable_func = comparator->build(arguments);
        auto data_type = executable_func->getResultType();
        res = executable_func->execute(arguments, data_type, input_rows_count, /* dry_run = */ false);

        return res;
    }
};
}
