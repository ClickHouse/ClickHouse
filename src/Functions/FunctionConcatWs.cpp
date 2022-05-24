#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Field.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>
#include <string>
#include <vector>


namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionConcatWs : public IFunction {
public:
    static constexpr auto name = "concatWs";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionConcatWs>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override 
    {
        if (arguments.size() <= 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeString>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto casted_column_des = castColumn(std::move(arguments[0]), string_type);
        const ColumnString * col_des = checkAndGetColumn<ColumnString>(casted_column_des.get()); 
        const ColumnString::Chars & vec_des = col_des-> getChars();
        const ColumnString::Offsets & offsets_des = col_des-> getOffsets();
       
        auto result_column = ColumnString::create();
        
        std::vector<std::string> ans(input_rows_count);
        for (size_t _ = 1; _ < arguments.size(); _++) {
            auto casted_column_temp = castColumn(std::move(arguments[_]), string_type);
            const ColumnString * col_temp = checkAndGetColumn<ColumnString>(casted_column_temp.get()); 
            const ColumnString::Chars & vec_temp = col_temp-> getChars();
            const ColumnString::Offsets & offsets_temp = col_temp-> getOffsets();

            size_t prev_offset_temp = 0;
            size_t prev_offset_des = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                for (size_t j = prev_offset_temp; j < offsets_temp[i]; j++) {
                    ans[i] += *reinterpret_cast<const char*>(&vec_temp[j]);
                }
                if (_ != arguments.size() - 1) {
                    for (size_t j = prev_offset_des; j < offsets_des[i]; j++) {
                        ans[i] += *reinterpret_cast<const uint8_t*>(&vec_des[j]);
                    }
                }

                prev_offset_temp = offsets_temp[i];
                prev_offset_des = offsets_des[i];
            }
        }
        for (size_t i = 0; i < input_rows_count; i++) {            
            result_column->insertData(ans[i].data(), ans[i].size());
        }
        return result_column;
    }
};

void registerFunctionConcatWs(FunctionFactory & factory) {
    factory.registerFunction<FunctionConcatWs>();   
}

}
