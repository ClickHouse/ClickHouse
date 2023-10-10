#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <Common/FieldVisitorToString.h>
#include "config.h"

#if USE_RAPIDJSON

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
    // select jsonMergePatch('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
    //           ||
    //           \/
    // ┌───────────────────────┐
    // │ {"a":1,"name":"zoey"} │
    // └───────────────────────┘
    class FunctionjsonMergePatch : public IFunction
    {
    public:
        static constexpr auto name = "jsonMergePatch";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionjsonMergePatch>(); }

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

            std::function<void(rapidjson::Value&, const rapidjson::Value&)> mergeObjects;
            mergeObjects = [&mergeObjects, &allocator](rapidjson::Value& dest, const rapidjson::Value& src) -> void
            {
                if (!src.IsObject())
                    return;
                for (auto it = src.MemberBegin(); it != src.MemberEnd(); ++it)
                {
                    rapidjson::Value key(it->name, allocator);
                    rapidjson::Value value(it->value, allocator);
                    if (dest.HasMember(key))
                    {
                        if (dest[key].IsObject() && value.IsObject())
                            mergeObjects(dest[key], value);
                        else
                            dest[key] = value;
                    }
                    else
                    {
                        dest.AddMember(key, value, allocator);
                    }
                }
            };

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
                    mergeObjects(merged_json, document);
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

REGISTER_FUNCTION(jsonMergePatch)
{
    factory.registerFunction<FunctionjsonMergePatch>(FunctionDocumentation{
        .description="Return the merged JSON object string, which is formed by merging multiple JSON objects."});
}

}

#endif
