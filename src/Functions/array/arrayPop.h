#pragma once
#include <Functions/IFunction.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class FunctionArrayPop : public IFunction
{
public:
    FunctionArrayPop(bool pop_front_, const char * name_) : pop_front(pop_front_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    String getSignatureString() const override
    {
        return "(Array(T)) -> Array(T) OR (NULL) -> NULL";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & return_type = result_type;

        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto & array_column = arguments[0].column;

        std::unique_ptr<GatherUtils::IArraySource> source;

        size_t size = array_column->size();

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            source = GatherUtils::createArraySource(*argument_column_array, false, size);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());

        ColumnArray::MutablePtr sink;

        if (pop_front)
            sink = GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, 1);
        else
            sink = GatherUtils::sliceFromLeftConstantOffsetBounded(*source, 0, -1);

        return sink;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    bool pop_front;
    const char * name;
};

}
