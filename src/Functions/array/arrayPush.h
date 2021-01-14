#pragma once
#include <Functions/IFunctionImpl.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array but it has type "
                            + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto nested_type = array_type->getNestedType();

        DataTypes types = {nested_type, arguments[1]};

        return std::make_shared<DataTypeArray>(getLeastSupertype(types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        auto result_column = return_type->createColumn();

        auto array_column = arguments[0].column;
        auto appended_column = arguments[1].column;

        if (!arguments[0].type->equals(*return_type))
            array_column = castColumn(arguments[0], return_type);

        const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
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
            throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};


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
