#pragma once
#include <Functions/IFunctionImpl.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array but it has type "
                            + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const auto & return_type = columns[result].type;

        if (return_type->onlyNull())
        {
            columns[result].column = return_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        const auto & array_column = columns[arguments[0]].column;

        std::unique_ptr<GatherUtils::IArraySource> source;

        size_t size = array_column->size();

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            source = GatherUtils::createArraySource(*argument_column_array, false, size);
        else
            throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};

        ColumnArray::MutablePtr sink;

        if (pop_front)
            sink = GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, 1);
        else
            sink = GatherUtils::sliceFromLeftConstantOffsetBounded(*source, 0, -1);

        columns[result].column = std::move(sink);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    bool pop_front;
    const char * name;
};

}
