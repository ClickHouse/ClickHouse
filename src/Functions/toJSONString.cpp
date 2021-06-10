#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace
{
    class FunctionToJSONString : public IFunction
    {
    public:
        static constexpr auto name = "toJSONString";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToJSONString>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeString>(); }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            auto res = ColumnString::create();
            ColumnString::Chars & data_to = res->getChars();
            ColumnString::Offsets & offsets_to = res->getOffsets();
            offsets_to.resize(input_rows_count);

            WriteBufferFromVector<ColumnString::Chars> json(data_to);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                arguments[0].type->getDefaultSerialization()->serializeTextJSON(*arguments[0].column, i, json, FormatSettings());
                writeChar(0, json);
                offsets_to[i] = json.count();
            }

            json.finalize();
            return res;
        }
    };
}

void registerFunctionToJSONString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToJSONString>();
}

}
