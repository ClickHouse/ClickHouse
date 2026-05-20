#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionArrayPush : public IFunction
{
public:
    FunctionArrayPush(bool push_front_, const char * name_)
        : push_front(push_front_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
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

        auto nested_type = array_type->getNestedType();

        DataTypes types = {nested_type, arguments[1]};

        DataTypePtr result = std::make_shared<DataTypeArray>(getLeastSupertype(types));
        if (arguments[0]->isNullable())
            return makeNullable(result);
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
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

                if (return_type->isNullable())
                {
                    auto mutable_nested_result = IColumn::mutate(nested_result->convertToFullColumnIfConst());
                    const size_t num_rows = col_const ? col_const->size() : mutable_nested_result->size();
                    if (mutable_nested_result->size() == 1 && num_rows != 1)
                        mutable_nested_result = mutable_nested_result->cloneResized(num_rows);

                    auto null_map = ColumnUInt8::create();
                    auto & null_map_data = null_map->getData();
                    null_map_data.assign(
                        nullable_array_column->getNullMapData().begin(), nullable_array_column->getNullMapData().end());
                    if (null_map_data.size() == 1)
                        null_map_data.resize_fill(num_rows, nullable_array_column->getNullMapData()[0]);
                    else if (null_map_data.size() != num_rows)
                        null_map_data.resize_fill(num_rows, 0);

                    return ColumnNullable::create(std::move(mutable_nested_result), std::move(null_map));
                }

                return nested_result;
            }
        }
        else if (argument_type_is_nullable)
        {
            ColumnsWithTypeAndName nested_arguments = arguments;
            nested_arguments[0].type = removeNullable(arguments[0].type);

            auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

            if (return_type->isNullable())
            {
                auto null_map = ColumnUInt8::create();
                null_map->getData().resize_fill(nested_result->size(), 0);
                return ColumnNullable::create(nested_result, std::move(null_map));
            }

            return nested_result;
        }

        auto result_column = removeNullable(return_type)->createColumn();

        auto array_column = arguments[0].column;
        auto appended_column = arguments[1].column;

        if (!arguments[0].type->equals(*return_type))
            array_column = castColumn(arguments[0], return_type);

        const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*removeNullable(return_type)).getNestedType();
        if (!arguments[1].type->equals(*return_nested_type))
            appended_column = castColumn(arguments[1], return_nested_type);

        std::unique_ptr<GatherUtils::IArraySource> array_source;
        std::unique_ptr<GatherUtils::IValueSource> value_source;

        size_t size = array_column->size();
        bool is_const = false;

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());


        bool is_appended_const = false;
        if (const auto * const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
        {
            is_appended_const = true;
            appended_column = const_appended_column->getDataColumnPtr();
        }

        value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

        auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

        GatherUtils::push(*array_source, *value_source, *sink, push_front);

        return result_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    bool push_front;
    const char * name;
};

}
