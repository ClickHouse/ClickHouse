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


class FunctionConv : public IFunction {
public:
    static constexpr auto name = "conv";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionConv>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 3; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override 
    {
        if (arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if ((!isNumber(arguments[0]) && !isString(arguments[0])) ||
            (!isNumber(arguments[1]) && !isString(arguments[1])) ||
            (!isNumber(arguments[2]) && !isString(arguments[2])))
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                                + ", must be Integer, String or Float number",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {   
        auto string_type1 = std::make_shared<DataTypeString>();
        auto casted_column_num  = castColumn(std::move(arguments[0]), string_type1);
        const ColumnString * col_num  = checkAndGetColumn<ColumnString>(casted_column_num.get()); 
        const ColumnString::Chars & vec_src = col_num-> getChars();
        const ColumnString::Offsets & offsets_src = col_num-> getOffsets();
        
        auto string_type2 = std::make_shared<DataTypeInt64>();
        auto casted_column_from = castColumn(std::move(arguments[1]), string_type2);
        const ColumnInt64 * column_concrete_from = checkAndGetColumn<ColumnInt64>(casted_column_from.get());
        const typename ColumnVector<Int64>::Container & num_from = column_concrete_from->getData();

        auto string_type3 = std::make_shared<DataTypeInt64>();
        auto casted_column_to = castColumn(std::move(arguments[2]), string_type3);
        const ColumnInt64 * column_concrete_to = checkAndGetColumn<ColumnInt64>(casted_column_to.get());
        const typename ColumnVector<Int64>::Container & num_to = column_concrete_to->getData();
        
        auto result_column = ColumnString::create();
        
        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string num = "";
            int64_t neg = 0;
            for (size_t j = prev_offset; j < offsets_src[i] - 1; j++) {
                if (j == prev_offset) {
                    const char temp = *reinterpret_cast<const char*>(&vec_src[j]);
                    if (temp == '-') {
                        neg++;
                        continue;
                    }
                }
                num += *reinterpret_cast<const char*>(&vec_src[j]);
            }
            prev_offset = offsets_src[i];

            int64_t ans = 0;
            int64_t base = num_from[i];
            if (base < 0) {
                base = -base;
            }
            for (size_t j = 0; j < num.size(); j++) {
                ans = ans * base;
                int64_t add = 0;
                if (num[j] >= '0' && num[j] <= '9') {
                    add = num[j] - '0';
                }
                if (num[j] >= 'a' && num[j] <= 'z') {
                    add = num[j] - 'a' + 10;
                }
                if (num[j] >= 'A' && num[j] <= 'Z') {
                    add = num[j] - 'A' + 10;
                }
                ans += add;
            }
            String ans_s;
            base = num_to[i];
            if (base < 0) {
                base = -base;
                neg++;
            }
            while (ans > 0) {
                int64_t add = ans % base;
                if (add < 10) {
                    ans_s += toString(add);
                } else {
                    char c = add - 10 + 'A';
                    ans_s += c;
                }
                ans /= base;
            }
            if (ans_s.size() != 0 && neg == 2) {
                ans_s += '-';
            }
            reverse(ans_s.begin(), ans_s.end());
            result_column->insertData(ans_s.data(), ans_s.size());
        }
        return result_column;
    }
};

void registerFunctionConv(FunctionFactory & factory) {
    factory.registerFunction<FunctionConv>();   
}

}
