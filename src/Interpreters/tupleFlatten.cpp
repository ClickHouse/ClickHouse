#include <Interpreters/tupleFlatten.h>

#include <memory>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Common/assert_cast.h>
#include <ext/map.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    // Extract element of tuple by predefined index. The operation is used only for tupleFlatten.
    class FunctionTupleElementWithIndex : public IFunction
    {
    public:
        static constexpr auto name = "tupleElementWithIndex";

        FunctionTupleElementWithIndex(const DataTypePtr & return_type_, size_t index_) : return_type(return_type_), index(index_) { }
        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }
        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return return_type; }

        void executeImpl(
            ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
        {
            Columns array_offsets;

            const auto & first_arg = columns[arguments[0]];

            const IDataType * tuple_type = first_arg.type.get();
            const IColumn * tuple_col = first_arg.column.get();
            while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(tuple_type))
            {
                const ColumnArray * array_col = assert_cast<const ColumnArray *>(tuple_col);

                tuple_type = array_type->getNestedType().get();
                tuple_col = &array_col->getData();
                array_offsets.push_back(array_col->getOffsetsPtr());
            }

            const DataTypeTuple * tuple_type_concrete = checkAndGetDataType<DataTypeTuple>(tuple_type);
            const ColumnTuple * tuple_col_concrete = checkAndGetColumn<ColumnTuple>(tuple_col);
            if (!tuple_type_concrete || !tuple_col_concrete)
                throw Exception(
                    "First argument for function " + getName() + " must be tuple or array of tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            ColumnPtr res = tuple_col_concrete->getColumns()[index];

            /// Wrap into Arrays
            for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
                res = ColumnArray::create(res, *it);

            columns[result].column = res;
        }

    private:
        DataTypePtr return_type;
        size_t index;
    };
}

DataTypePtr TupleElementWithIndexOverloadResolver::getReturnType(const ColumnsWithTypeAndName & arguments) const
{
    size_t count_arrays = 0;

    const IDataType * tuple_col = arguments[0].type.get();
    while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(tuple_col))
    {
        tuple_col = array->getNestedType().get();
        ++count_arrays;
    }

    const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(tuple_col);
    if (!tuple)
        throw Exception(
            "First argument for function " + getName() + " must be tuple or array of tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypePtr out_return_type = tuple->getElements()[index - 1];

    for (; count_arrays; --count_arrays)
        out_return_type = std::make_shared<DataTypeArray>(out_return_type);

    return out_return_type;
}

FunctionBaseImplPtr
TupleElementWithIndexOverloadResolver::build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const
{
    if (index == 0)
        throw Exception("Cannot build function " + getName() + " because tuple index is not initialized yet", ErrorCodes::LOGICAL_ERROR);
    return std::make_unique<DefaultFunction>(
        std::make_unique<FunctionTupleElementWithIndex>(return_type, index - 1),
        ext::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
        return_type);
}

}
