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


class FunctionElt : public IFunction {
public:
    static constexpr auto name = "elt";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionElt>(); }
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
        auto string_type = std::make_shared<DataTypeInt32>();
        auto casted_column = castColumn(std::move(arguments[0]), string_type);
        const ColumnInt32 * column_concrete = checkAndGetColumn<ColumnInt32>(casted_column.get());
        const typename ColumnVector<Int32>::Container & idx_str = column_concrete->getData();
       
        auto result_column = ColumnString::create();
        
        std::vector<std::string> ans(input_rows_count);
        std::vector<bool> empty_string(input_rows_count, true);
        for (size_t _ = 1; _ < arguments.size(); _++) {
            auto string_type1 = std::make_shared<DataTypeString>();
            auto casted_column_temp = castColumn(std::move(arguments[_]), string_type1);
            const ColumnString * col_temp = checkAndGetColumn<ColumnString>(casted_column_temp.get()); 
            const ColumnString::Chars & vec_temp = col_temp-> getChars();
            const ColumnString::Offsets & offsets_temp = col_temp-> getOffsets();

            size_t prev_offset_temp = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                for (size_t j = prev_offset_temp; j < offsets_temp[i]; j++) {
                    int32_t temp_cast = _;
                    if (temp_cast == idx_str[i]) {
                        ans[i] += *reinterpret_cast<const char*>(&vec_temp[j]);
                        empty_string[i] = false;
                    }
                }
                prev_offset_temp = offsets_temp[i];
            }
        }
        for (size_t i = 0; i < input_rows_count; i++) {
            if (empty_string[i]) {
                result_column->insertData(nullptr, 0);
            } else {
                result_column->insertData(ans[i].data(), ans[i].size());
            }
        }
        return result_column;
    }
};

void registerFunctionElt(FunctionFactory & factory) {
    factory.registerFunction<FunctionElt>();   
}

}
