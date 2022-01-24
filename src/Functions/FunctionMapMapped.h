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
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Higher-order functions for map.
  * These functions optionally apply a map by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * mapMap(x1,...,xn -> expression, map) - apply the expression to  the map.
  * mapFilter((k, v) -> predicate, map) - leave in the map only the kv elements for which the expression is true.
  */
template <typename Impl, typename Name>
class FunctionMapMapped : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapMapped>(); }

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
        if (arguments.empty())
            throw Exception("Function " + getName() + " needs at least one argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
            throw Exception("Function " + getName() + " needs at least one map argument.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types((arguments.size() - 1) * 2);
        for (size_t i = 0; i < arguments.size() - 1; ++i)
        {
            const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(&*arguments[i + 1]);
            if (!map_type)
                throw Exception("Argument " + toString(i + 2) + " of function " + getName() + " must be map. Found "
                                + arguments[i + 1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i * 2] = recursiveRemoveLowCardinality(map_type->getKeyType());
            nested_types[i * 2 + 1] = recursiveRemoveLowCardinality(map_type->getValueType());
        }

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception("First argument for this overload of " + getName() + " must be a function with "
                            + toString(nested_types.size()) + " arguments. Found "
                            + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t min_args = Impl::needExpression() ? 2 : 1;
        if (arguments.size() < min_args)
            throw Exception("Function " + getName() + " needs at least "
                            + toString(min_args) + " argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (arguments.size() > 2 && Impl::needOneMap())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' needs one map argument", getName());

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

        ColumnPtr offsets_column;

        ColumnPtr column_first_map_ptr;
        const ColumnMap * column_first_map = nullptr;

        ColumnsWithTypeAndName maps;
        maps.reserve(arguments.size() - 1);

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto & map_with_type_and_name = arguments[i];

            ColumnPtr column_map_ptr = map_with_type_and_name.column;
            const auto * column_map = checkAndGetColumn<ColumnMap>(column_map_ptr.get());

            const DataTypePtr & map_type_ptr = map_with_type_and_name.type;
            const auto * map_type = checkAndGetDataType<DataTypeMap>(map_type_ptr.get());

            if (!column_map)
            {
                const ColumnConst * column_const_map = checkAndGetColumnConst<ColumnMap>(column_map_ptr.get());
                if (!column_const_map)
                    throw Exception("Expected map column, found " + column_map_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                column_map_ptr = recursiveRemoveLowCardinality(column_const_map->convertToFullColumn());
                column_map = checkAndGetColumn<ColumnMap>(column_map_ptr.get());
            }

            if (!map_type)
                throw Exception("Expected map type, found " + map_type_ptr->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!offsets_column)
            {
                offsets_column = column_map->getNestedColumn().getOffsetsPtr();
            }
            else
            {
                /// The first condition is optimization: do not compare data if the pointers are equal.
                if (column_map->getNestedColumn().getOffsetsPtr() != offsets_column
                    && column_map->getNestedColumn().getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                    throw Exception("maps passed to " + getName() + " must have equal size", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }

            if (i == 1)
            {
                column_first_map_ptr = column_map_ptr;
                column_first_map = column_map;
            }

            maps.emplace_back(ColumnWithTypeAndName(
                column_map->getNestedData().getColumnPtr(0), recursiveRemoveLowCardinality(map_type->getKeyType()), map_with_type_and_name.name+".key"));
            maps.emplace_back(ColumnWithTypeAndName(
                column_map->getNestedData().getColumnPtr(1), recursiveRemoveLowCardinality(map_type->getValueType()), map_with_type_and_name.name+".value"));
        }

        /// Put all the necessary columns multiplied by the sizes of maps into the columns.
        auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(column_first_map->getNestedColumn().getOffsets()));
        auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
        replicated_column_function->appendArguments(maps);

        auto lambda_result = replicated_column_function->reduce().column;
        if (lambda_result->lowCardinality())
            lambda_result = lambda_result->convertToFullColumnIfLowCardinality();

        return Impl::execute(*column_first_map, lambda_result);
    }
};

}
