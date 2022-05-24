#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Field.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>
#include <../src/IO/ReadHelpers.h>
#include <Functions/FunctionsMultiStringFuzzySearch.h>


static UInt32 z_function(String s, String s1) {
    int block_len = s1.size();
    s = s1 + "#" + s;
	int n = s.size();
	std::vector<int> z(n);
	for (int i = 1, l = 0, r = 0; i < n; ++i) {
		if (i <= r)
			z[i] = std::min(r - i + 1, z[i - l]);
		while (i + z[i] < n && s[z[i]] == s[i + z[i]] && z[i] < block_len)
        {
			++z[i];
        }
		if (i + z[i] - 1 > r)
			l = i;
            r = i + z[i] - 1;
	}
    for (int i = block_len; i < n; ++i) {
        if (z[i] == block_len) {
            return i - block_len;
        }
    }
	return 0;
}
namespace DB {

class FunctionInstr : public IFunction {
public:
    static constexpr auto name = "instr";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionInstr>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes &arguments) const override 
    {
         if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH); 
            
        return std::make_shared<DataTypeUInt32>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto string_type = std::make_shared<DataTypeString>();

        auto casted_column = castColumn(std::move(arguments[0]), string_type);
        const ColumnString * col = checkAndGetColumn<ColumnString>(casted_column.get()); 
        
        casted_column = castColumn(std::move(arguments[1]), string_type);
        const ColumnString * col1 = checkAndGetColumn<ColumnString>(casted_column.get());


        const ColumnString::Chars & vec_src = col-> getChars();
        const ColumnString::Offsets & offsets_src = col-> getOffsets();
        size_t prev_offset = 0;

        const ColumnString::Chars & vec_src1 = col1-> getChars();
        const ColumnString::Offsets & offsets_src1 = col1-> getOffsets();
        size_t prev_offset1 = 0;

        auto col_res = ColumnUInt32::create();
        ColumnUInt32::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int nxt = offsets_src[i];
            int nxt1 = offsets_src1[i];
            String s, s1;
            for (int j = prev_offset; j < nxt - 1; ++j) {
                s+= *reinterpret_cast<const char*>(&vec_src[j]);
            }
            for (int j = prev_offset1; j < nxt1 - 1; ++j) {
                s1+= *reinterpret_cast<const char*>(&vec_src1[j]);
            }

            vec_res[i] = z_function(s, s1);
            
            prev_offset = nxt;
            prev_offset1 = nxt1;
            
        }
        return col_res;
    }
};

void registerFunctionInstr(FunctionFactory & factory) {
    factory.registerFunction<FunctionInstr>();   
}

}
