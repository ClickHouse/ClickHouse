#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeWithDictionary.h>
#include <Columns/ColumnWithDictionary.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionLowCardinalityKeys: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityKeys";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLowCardinalityKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto * type = typeid_cast<const DataTypeWithDictionary *>(arguments[0].get());
        if (!type)
            throw Exception("First first argument of function lowCardinalityKeys must be ColumnWithDictionary, but got"
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getDictionaryType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);
        const auto * column_with_dictionary = typeid_cast<const ColumnWithDictionary *>(arg.column.get());
        res.column = column_with_dictionary->getDictionary().getNestedColumn()->cloneResized(arg.column->size());
    }
};


void registerFunctionLowCardinalityKeys(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLowCardinalityKeys>();
}

}
