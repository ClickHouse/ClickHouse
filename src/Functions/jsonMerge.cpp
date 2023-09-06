#include "config.h"

#if USE_RAPIDJSON

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <Common/FieldVisitorToString.h>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/filereadstream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_JSON_OBJECT_FORMAT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    // select jsonMerge('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
    //           ||
    //           \/
    // ┌───────────────────────┐
    // │ {"a":1,"name":"zoey"} │
    // └───────────────────────┘
    class FunctionjsonMerge : public IFunction
    {
    public:
        static constexpr auto name = "jsonMerge";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionjsonMerge>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }
        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (arguments.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            rapidjson::Document merged_json;
            merged_json.SetObject();
            rapidjson::Document::AllocatorType& allocator = merged_json.GetAllocator();

            for (const auto & arg : arguments)
            {
                const ColumnPtr column = arg.column;
                const ColumnString * col = typeid_cast<const ColumnString *>(column.get());
                if (!col)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be string", getName());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    auto str_ref = col->getDataAt(i);
                    const char* json = str_ref.data;
                    rapidjson::Document document;
                    document.Parse(json);
                    if (!document.IsObject())
                        throw Exception(ErrorCodes::ILLEGAL_JSON_OBJECT_FORMAT, "Wrong input Json object format");

                    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
                    {
                        rapidjson::Value key(it->name, allocator);
                        rapidjson::Value value(it->value, allocator);
                        if (merged_json.HasMember(key))
                            merged_json[key] = value;
                        else
                            merged_json.AddMember(key, value, allocator);
                    }
                }
            }

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            merged_json.Accept(writer);
            std::string json_string = buffer.GetString();

            auto res = ColumnString::create();
            res->insertData(json_string.c_str(), json_string.size());

            return res;
        }
    };

}

REGISTER_FUNCTION(jsonMerge)
{
    factory.registerFunction<FunctionjsonMerge>(FunctionDocumentation{
        .description="Return the merged JSON object, which is formed by merging multiple JSON objects."});
}

}

#endif
