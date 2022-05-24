#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Field.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>


namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionBitLength : public IFunction {
public:
    static constexpr auto name = "bitLength";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionBitLength>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override 
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNumber(arguments[0]) && !isString(arguments[0]))
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                                + ", must be Integer, String or Float number",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt32>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        
        auto string_type = std::make_shared<DataTypeString>();
        auto casted_column = castColumn(std::move(arguments[0]), string_type);
        
        const ColumnString * col = checkAndGetColumn<ColumnString>(casted_column.get()); 

        auto col_res = ColumnUInt32::create();
        ColumnUInt32::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnString::Offsets & offsets_src = col-> getOffsets();
        size_t prev_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            uint32_t len = offsets_src[i] - prev_offset - 1;
            vec_res[i] = len * 8;
            prev_offset = offsets_src[i];
        }
        return col_res;
    }
};

void registerFunctionBitLength(FunctionFactory & factory) {
    factory.registerFunction<FunctionBitLength>();   
}

}
