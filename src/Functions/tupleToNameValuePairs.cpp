#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_INDEX;
}

namespace
{

/** Transform a named tuple into an array of pairs, where the first element
  * of the pair corresponds to the tuple field name and the second one to the
  * tuple value.
  */
class FunctionTupleToNameValuePairs : public IFunction
{
public:
    static constexpr auto name = "tupleToNameValuePairs";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTupleToNameValuePairs>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // get the type of all the fields in the tuple
        const IDataType * col = arguments[0].type.get();
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(col);

        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be a tuple.",
                            getName());

        const auto & elementTypes = tuple->getElements();

        if (elementTypes.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument tuple for function {} must not be empty.",
                            getName());

        const auto & firstElementType = elementTypes[0];

        bool allValueTypesEqual = std::all_of(elementTypes.begin() + 1,
                                              elementTypes.end(),
                                              [&](const auto &other)
                                              {
                                                  return firstElementType->equals(*other);
                                              });

        if (!allValueTypesEqual)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument tuple for function {} must contain just one type.",
                            getName());
        }

        DataTypePtr tupleNameType = std::make_shared<DataTypeString>();
        DataTypes itemDataTypes ={tupleNameType,
                                  firstElementType};

        auto itemDataType = std::make_shared<DataTypeTuple>(itemDataTypes);

        return std::make_shared<DataTypeArray>(itemDataType);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * tuple_col = arguments[0].column.get();
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        auto * tuple_col_concrete = assert_cast<const ColumnTuple*>(tuple_col);

        auto keys = ColumnString::create();
        MutableColumnPtr values = tuple_col_concrete->getColumn(0).cloneEmpty();
        auto offsets = ColumnVector<UInt64>::create();
        for (size_t row = 0; row < tuple_col_concrete->size(); ++row)
        {
            for (size_t col = 0; col < tuple_col_concrete->tupleSize(); ++col)
            {
                const std::string & key = tuple->getElementNames()[col];
                const IColumn & valueColumn = tuple_col_concrete->getColumn(col);

                values->insertFrom(valueColumn, row);
                keys->insertData(key.data(), key.size());
            }
            offsets->insertValue(tuple_col_concrete->tupleSize() * (row + 1));
        }

        std::vector<ColumnPtr> tupleColumns = { std::move(keys), std::move(values) };
        auto tupleColumn = ColumnTuple::create(std::move(tupleColumns));
        return ColumnArray::create(std::move(tupleColumn), std::move(offsets));
    }
};

}

void registerFunctionTupleToNameValuePairs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTupleToNameValuePairs>();
}

}
