#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionArrayPop : public IFunction
{
public:
    FunctionArrayPop(bool pop_front_, const char * name_) : pop_front(pop_front_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = typeid_cast<const DataTypeArray *>(removeNullable(arguments[0]).get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be an array but it has type {}.",
                            getName(), arguments[0]->getName());

        return arguments[0];
    }

    ColumnPtr executeOnArrayColumn(const ColumnPtr & array_column, size_t input_rows_count) const
    {
        auto column = array_column;
        bool is_const = false;
        size_t size = column->size();

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(column.get()))
        {
            is_const = true;
            column = const_array_column->getDataColumnPtr();
            size = input_rows_count;
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(column.get()))
        {
            auto source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
            if (pop_front)
                return GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, 1);
            return GatherUtils::sliceFromLeftConstantOffsetBounded(*source, 0, -1);
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & return_type = result_type;

        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
        const IColumn * data_column = col_const ? &col_const->getDataColumn() : arguments[0].column.get();

        const bool argument_type_is_nullable = arguments[0].type->isNullable();

        if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(data_column))
        {
            if (checkAndGetColumn<ColumnArray>(&nullable_array_column->getNestedColumn()))
            {
                ColumnsWithTypeAndName nested_arguments = arguments;
                nested_arguments[0].column = col_const
                    ? ColumnConst::create(nullable_array_column->getNestedColumnPtr(), col_const->size())
                    : nullable_array_column->getNestedColumnPtr();
                nested_arguments[0].type = removeNullable(arguments[0].type);

                auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

                if (!return_type->isNullable())
                    return nested_result;

                auto null_map = ColumnUInt8::create();
                null_map->getData().assign(
                    nullable_array_column->getNullMapData().begin(), nullable_array_column->getNullMapData().end());
                return ColumnNullable::create(nested_result, std::move(null_map));
            }
        }
        else if (argument_type_is_nullable)
        {
            /// Type is Nullable(Array) but column is bare Array (e.g. after partial constant folding).
            ColumnsWithTypeAndName nested_arguments = arguments;
            nested_arguments[0].type = removeNullable(arguments[0].type);

            auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

            if (!return_type->isNullable())
                return nested_result;

            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(nested_result->size(), 0);
            return ColumnNullable::create(nested_result, std::move(null_map));
        }

        auto result = executeOnArrayColumn(arguments[0].column, input_rows_count);

        if (return_type->isNullable() && !isColumnNullable(*result))
        {
            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(result->size(), 0);
            return ColumnNullable::create(result, std::move(null_map));
        }

        return result;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    bool pop_front;
    const char * name;
};

}
