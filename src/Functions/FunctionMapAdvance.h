#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Higher-order functions for map.
  * These functions optionally apply a map by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * mapApply(x1,...,xn -> expression, map) - apply the expression to  the map.
  * mapFilter((k, v) -> predicate, map) - leave in the map only the kv elements for which the expression is true.
  */
template <typename Impl, typename Name>
class FunctionMapAdvance : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapAdvance>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Called if at least one function argument is a lambda expression.
    /// For argument-lambda expressions, it defines the types of arguments of these expressions.
    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Function " + getName() + " needs 2 arguments.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types(2);
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(&*arguments[1]);
        if (!map_type)
            throw Exception("The second argument of function " + getName() + " must be map. Found "
                            + arguments[1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        nested_types[0] = recursiveRemoveLowCardinality(map_type->getKeyType());
        nested_types[1] = recursiveRemoveLowCardinality(map_type->getValueType());

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != 2)
            throw Exception("First argument for this overload of " + getName() + " must be a function with 2 arguments. Found "
                            + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Function " + getName() + " needs at least 2 argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());

        if (!data_type_function)
            throw Exception("First argument for function " + getName() + " must be a function.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.
        DataTypePtr return_type = removeLowCardinality(data_type_function->getReturnType());
        if (Impl::needBoolean() && !WhichDataType(return_type).isUInt8())
            throw Exception(
                "Expression for function " + getName() + " must return UInt8, found " + return_type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());
        if (!map_type)
            throw Exception("Second argument for function " + getName() + " must be a map.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return Impl::getReturnType(return_type, map_type->getKeyValueTypes());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & column_with_type_and_name = arguments[0];
        if (!column_with_type_and_name.column)
            throw Exception("First argument for function " + getName() + " must be a function.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * column_function = typeid_cast<const ColumnFunction *>(column_with_type_and_name.column.get());
        if (!column_function)
            throw Exception("First argument for function " + getName() + " must be a function.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        ColumnPtr column_map_ptr = arguments[1].column;
        const auto * column_map = checkAndGetColumn<ColumnMap>(column_map_ptr.get());
        if (!column_map)
        {
            const ColumnConst * column_const_map = checkAndGetColumnConst<ColumnMap>(column_map_ptr.get());
            if (!column_const_map)
                throw Exception("Expected map column, found " + column_map_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
            column_map_ptr = recursiveRemoveLowCardinality(column_const_map->convertToFullColumn());
            column_map = checkAndGetColumn<ColumnMap>(column_map_ptr.get());
        }

        const DataTypePtr & map_type_ptr = arguments[1].type;
        const auto * map_type = checkAndGetDataType<DataTypeMap>(map_type_ptr.get());
        const auto & offsets_column = column_map->getNestedColumn().getOffsets();

        /// Put all the necessary columns multiplied by the sizes of maps into the columns.
        auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(offsets_column));
        auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
        const ColumnsWithTypeAndName args {
            {column_map->getNestedData().getColumnPtr(0), recursiveRemoveLowCardinality(map_type->getKeyType()), arguments[1].name + ".key"},
            {column_map->getNestedData().getColumnPtr(1), recursiveRemoveLowCardinality(map_type->getValueType()), arguments[1].name + ".value"}};
        replicated_column_function->appendArguments(args);

        auto lambda_result = replicated_column_function->reduce().column;
        if (lambda_result->lowCardinality())
            lambda_result = lambda_result->convertToFullColumnIfLowCardinality();

        return Impl::execute(*column_map, lambda_result);
    }
};

}
