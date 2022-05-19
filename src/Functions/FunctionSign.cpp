/*#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Field.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>


namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionSign : public IFunction {
public:
    static constexpr auto name = "sign";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionSign>(); }
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

        if (!isInteger(arguments[0]) && !isString(arguments[0]) && !isFloat(arguments[0]))
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                                + ", must be Integer, String or Float number",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt8>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {   
        auto number_type = std::make_shared<DataTypeInt8>();
        auto casted_column = castColumn(std::move(arguments[0]), number_type);
        auto col_res = ColumnInt8::create();
        ColumnInt8::Container & vec_res = col_res->getData();
        vec_res.resize(casted_column->size());

        const auto & src = arguments[0];
        const auto & col = *src.column;
        const auto & source_data = typeid_cast<const ColumnDecimal<DataTypeInt256> &>(col).getData();

        for (size_t i = 0; i < input_rows_count; ++i) 
        {
            if (source_data[i] < 0) {
                vec_res[i] = -1;    
            }
            if (source_data[i] > 0) {
                vec_res[i] = 1;    
            }
            if (source_data[i] == 0) {
                vec_res[i] = 0;    
            }
        }
        return col_res;
    }
};

void registerFunctionASCII(FunctionFactory & factory) {
    factory.registerFunction<FunctionSign>();   
}

}*/
