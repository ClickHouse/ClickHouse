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


namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static std::vector<UInt64> Borders = {1, static_cast<UInt64>(1) << 10, static_cast<UInt64>(1) << 20, static_cast<UInt64>(1) << 30, 
                    static_cast<UInt64>(1) << 40, static_cast<UInt64>(1) << 50, 
                    static_cast<UInt64>(1) << 60};

static String NameOfBorders[] = {"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};


class FunctionFormatBytes : public IFunction {
public:
    static constexpr auto name = "formatBytes";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionFormatBytes>(); }
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

        if (!isInteger(arguments[0]) && !isFloat(arguments[0]) && !isString(arguments[0]))
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                                + ", must be Integer, String or Float number",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>(); 
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto casted_column = castColumn(std::move(arguments[0]), string_type);
        
        const ColumnString * col = checkAndGetColumn<ColumnString>(casted_column.get()); 
        auto result_column = ColumnString::create();

        const ColumnString::Chars & vec_src = col-> getChars();
        const ColumnString::Offsets & offsets_src = col-> getOffsets();
        size_t prev_offset = 0;
        
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int nxt = offsets_src[i];
            String ans;
            for (int j = prev_offset; j < nxt; ++j) {
                ans+= *reinterpret_cast<const char*>(&vec_src[j]);
            }
            UInt64 number = 0;
            if (!tryParse<UInt64>(number, ans.data(), ans.size())) {
                number = 0;
            }
            prev_offset = nxt;

            size_t pos = Borders.size();
            for (size_t idx = 1; idx < Borders.size() - 1; ++idx) {
                if (number < Borders[idx]) {
                    pos = idx;
                    break;
                }
            }
            Float64 otherNumber = static_cast<Float64>(number) / static_cast<Float64>(Borders[pos - 1]);
            ans = "";
            if (pos != 1) {
                UInt64 Dec = static_cast<UInt64>(round(otherNumber * 100));
                UInt8 r = Dec % 100;
                ans = toString(Dec / 100) + ".";
                if (r < 10) {
                    ans += "0";
                }
                ans += toString(r) + " " + NameOfBorders[pos - 1];
            } else {
                ans = toString(otherNumber) + " " + NameOfBorders[pos - 1];
            }
            result_column->insertData(ans.data(), ans.size());
        }
        return result_column;
    }
};

void registerFunctionFormatBytes(FunctionFactory & factory) {
    factory.registerFunction<FunctionFormatBytes>();   
}

}
