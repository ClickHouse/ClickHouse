#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeWithDictionary.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnWithDictionary.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionLowCardinalityIndices: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityIndices";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLowCardinalityIndices>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto * type = typeid_cast<const DataTypeWithDictionary *>(arguments[0].get());
        if (!type)
            throw Exception("First first argument of function lowCardinalityIndexes must be ColumnWithDictionary, but got"
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);
        auto indexes_col = typeid_cast<const ColumnWithDictionary *>(arg.column.get())->getIndexesPtr();
        auto new_indexes_col = ColumnUInt64::create(indexes_col->size());
        auto & data = new_indexes_col->getData();
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = indexes_col->getUInt(i);

        res.column = std::move(new_indexes_col);
    }
};


void registerFunctionLowCardinalityIndices(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLowCardinalityIndices>();
}

}
