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

/// Prevent stack overflow:
#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/error/en.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    // select JSONMergePatch('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
    //           ||
    //           \/
    // ┌───────────────────────┐
    // │ {"a":1,"name":"zoey"} │
    // └───────────────────────┘
    class FunctionJSONMergePatch : public IFunction
    {
    public:
        static constexpr auto name = "JSONMergePatch";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONMergePatch>(); }

        String getName() const override { return name; }
        bool isVariadic() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }
        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (arguments.empty())
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires at least one argument.", getName());

            for (const auto & arg : arguments)
                if (!isString(arg.type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires string arguments", getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            chassert(!arguments.empty());

            rapidjson::Document::AllocatorType allocator;
            std::function<void(rapidjson::Value &, const rapidjson::Value &)> merge_objects;

            merge_objects = [&merge_objects, &allocator](rapidjson::Value & dest, const rapidjson::Value & src) -> void
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
                            merge_objects(dest[key], value);
                        else
                            dest[key] = value;
                    }
                    else
                    {
                        dest.AddMember(key, value, allocator);
                    }
                }
            };

            auto parse_json_document = [](const ColumnString & column, rapidjson::Document & document, size_t i)
            {
                auto str_ref = column.getDataAt(i);
                const char * json = str_ref.data;

                document.Parse(json);

                if (document.HasParseError())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong JSON string to merge: {}", rapidjson::GetParseError_En(document.GetParseError()));

                if (!document.IsObject())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong JSON string to merge. Expected JSON object");
            };

            const bool is_first_const = isColumnConst(*arguments[0].column);
            const auto * first_column_arg_string = is_first_const
                        ? checkAndGetColumnConstData<ColumnString>(arguments[0].column.get())
                        : checkAndGetColumn<ColumnString>(arguments[0].column.get());

            if (!first_column_arg_string)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be strings", getName());

            std::vector<rapidjson::Document> merged_jsons;
            merged_jsons.reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto & merged_json = merged_jsons.emplace_back(rapidjson::Type::kObjectType, &allocator);
                if (is_first_const)
                    parse_json_document(*first_column_arg_string, merged_json, 0);
                else
                    parse_json_document(*first_column_arg_string, merged_json, i);
            }

            for (size_t col_idx = 1; col_idx < arguments.size(); ++col_idx)
            {
                const bool is_const = isColumnConst(*arguments[col_idx].column);
                const auto * column_arg_string = is_const
                            ? checkAndGetColumnConstData<ColumnString>(arguments[col_idx].column.get())
                            : checkAndGetColumn<ColumnString>(arguments[col_idx].column.get());

                if (!column_arg_string)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be strings", getName());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    rapidjson::Document document(&allocator);
                    if (is_const)
                        parse_json_document(*column_arg_string, document, 0);
                    else
                        parse_json_document(*column_arg_string, document, i);
                    merge_objects(merged_jsons[i], document);
                }
            }

            auto result = ColumnString::create();
            auto & result_string = assert_cast<ColumnString &>(*result);
            rapidjson::CrtAllocator buffer_allocator;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                rapidjson::StringBuffer buffer(&buffer_allocator);
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

                merged_jsons[i].Accept(writer);
                result_string.insertData(buffer.GetString(), buffer.GetSize());
            }

            return result;
        }
    };

}

REGISTER_FUNCTION(JSONMergePatch)
{
    factory.registerFunction<FunctionJSONMergePatch>(FunctionDocumentation{
        .description="Returns the merged JSON object string, which is formed by merging multiple JSON objects."});

    factory.registerAlias("jsonMergePatch", "JSONMergePatch");
}

}

#endif
