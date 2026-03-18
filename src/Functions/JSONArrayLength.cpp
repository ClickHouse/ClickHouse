#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include "config.h"

#if USE_SIMDJSON
#    include <Common/JSONParsers/SimdJSONParser.h>
#elif USE_RAPIDJSON
#    include <Common/JSONParsers/RapidJSONParser.h>
#else
#    include <Common/JSONParsers/DummyJSONParser.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    /// JSONArrayLength(json)
    class FunctionJSONArrayLength : public IFunction
    {
    public:
        static constexpr auto name = "JSONArrayLength";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONArrayLength>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            auto args = FunctionArgumentDescriptors{
                {"json", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            };

            validateFunctionArguments(*this, arguments, args);
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnPtr column = arguments[0].column;
            const ColumnString * col = typeid_cast<const ColumnString *>(column.get());
            if (!col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be string", getName());

            auto null_map = ColumnUInt8::create();
            auto data = ColumnUInt64::create();
            null_map->reserve(input_rows_count);
            data->reserve(input_rows_count);

#if USE_SIMDJSON
            SimdJSONParser parser;
            SimdJSONParser::Element element;
#elif USE_RAPIDJSON
            RapidJSONParser parser;
            RapidJSONParser::Element element;
#else
            DummyJSONParser parser;
            DummyJSONParser::Element element;
#endif

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto str_ref = col->getDataAt(i);
                std::string_view str_view(str_ref.data, str_ref.size);
                bool ok = parser.parse(std::move(str_view), element);
                if (!ok || !element.isArray())
                {
                    null_map->insertValue(1);
                    data->insertDefault();
                }
                else
                {
                    auto array = element.getArray();
                    null_map->insertValue(0);
                    data->insertValue(array.size());
                }
            }
            return ColumnNullable::create(std::move(data), std::move(null_map));
        }
    };

}

REGISTER_FUNCTION(JSONArrayLength)
{
    factory.registerFunction<FunctionJSONArrayLength>(FunctionDocumentation{
        .description="Returns the number of elements in the outermost JSON array. The function returns NULL if input JSON string is invalid."});

    /// For Spark compatibility.
    factory.registerAlias("JSON_ARRAY_LENGTH", "JSONArrayLength", FunctionFactory::Case::Insensitive);
}

}
