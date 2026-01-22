#pragma once
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsComparison.h>
#include <Columns/ColumnNullable.h>
#include <Common/quoteString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
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
class FunctionsNullSafeCmp : public IFunction
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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types of arguments ({}, {})"
                " of function {}", backQuote(arguments[0]->getName()), backQuote(arguments[1]->getName()), backQuote(getName()));

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr ALWAYS_INLINE executeForVariantOrDynamicAndNull(const ColumnWithTypeAndName & variant_or_dynamic_col) const
    {
        const auto & column_variant_or_dynamic =
            isVariant(variant_or_dynamic_col.type) ?
                checkAndGetColumn<ColumnVariant>(*variant_or_dynamic_col.column) :
                checkAndGetColumn<ColumnDynamic>(*variant_or_dynamic_col.column).getVariantColumn();
        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        data.resize(column_variant_or_dynamic.size());
        for (size_t i = 0; i < column_variant_or_dynamic.size(); ++i)
        {
            bool ele_is_null = column_variant_or_dynamic.isNullAt(i);
            data[i] = is_equal_mode ? ele_is_null && true : ele_is_null && false;
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

        // for self null-safe cmp
        if (type_and_name_left_col.name == type_and_name_right_col.name
            && type_and_name_left_col.type->equals(*type_and_name_right_col.type)
            && !isTuple(type_and_name_left_col.type)
            && left_col.get() == right_col.get())
        {
            return is_equal_mode ? result_type->createColumnConst(input_rows_count, UInt8(1)) :
                                    result_type->createColumnConst(input_rows_count, UInt8(0));
        }

        // To address:
        //   1. Map vs null or
        //   2. Array vs null
        // The results will be always set to 0
        if (((isMap(type_and_name_left_col.type) || isArray(type_and_name_left_col.type))
                && type_and_name_right_col.type->onlyNull())
            || ((isMap(type_and_name_right_col.type) || isArray(type_and_name_right_col.type))
                && type_and_name_left_col.type->onlyNull()))
        {
            return result_type->createColumnConst(input_rows_count, UInt8(0));
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

        // get common type for null-safe comparison
        DataTypePtr common_type = getLeastSupertype(DataTypes{arguments[0].type, arguments[1].type});

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

        // To address normal
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
