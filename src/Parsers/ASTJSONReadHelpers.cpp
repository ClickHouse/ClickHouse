#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFromJSON.h>
#include <IO/ReadHelpers.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Compute the maximum bracket nesting depth of a `Field::restoreFromDump` payload.
/// `restoreFromDump` recursively parses `Array_[...]`, `Tuple_(...)`, `Map_(...)` and
/// `AggregateFunctionState_(...)` payloads; without a depth bound, a hostile JSON
/// `value` string can drive unbounded recursion regardless of the JSON object depth.
/// Quoted strings (single quotes, with backslash escapes) are skipped so that brackets
/// inside string literals do not count.
size_t computeFieldDumpNestingDepth(std::string_view dump)
{
    size_t depth = 0;
    size_t max_depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (char c : dump)
    {
        if (in_string)
        {
            if (escaped)
                escaped = false;
            else if (c == '\\')
                escaped = true;
            else if (c == '\'')
                in_string = false;
            continue;
        }
        if (c == '\'')
            in_string = true;
        else if (c == '[' || c == '(')
        {
            ++depth;
            max_depth = std::max(max_depth, depth);
        }
        else if ((c == ']' || c == ')') && depth > 0)
            --depth;
    }
    return max_depth;
}

}

Field JSONObjectReader::readFieldFromObject(const Poco::JSON::Object & obj)
{
    String field_type = obj.getValue<String>("field_type");

    if (field_type == "Null")
    {
        if (obj.isNull("value"))
            return Field(Null{});
        String val = obj.getValue<String>("value");
        if (val == "-Inf")
            return Field(NEGATIVE_INFINITY);
        if (val == "+Inf")
            return Field(POSITIVE_INFINITY);
        return Field(Null{});
    }
    if (field_type == "UInt64")
        return Field(static_cast<UInt64>(obj.getValue<Poco::UInt64>("value")));
    if (field_type == "Int64")
        return Field(static_cast<Int64>(obj.getValue<Poco::Int64>("value")));
    if (field_type == "Float64")
        return Field(obj.getValue<double>("value"));
    if (field_type == "Bool")
        return Field(obj.getValue<bool>("value"));
    if (field_type == "String")
        return Field(obj.getValue<String>("value"));

    if (field_type == "Array")
    {
        auto json_arr = obj.getArray("value");
        if (!json_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected JSON array for `value` of Array field during AST JSON deserialization");
        Array arr;
        arr.reserve(json_arr->size());
        for (unsigned int i = 0; i < json_arr->size(); ++i)
        {
            auto elem = json_arr->getObject(i);
            if (!elem)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in Array field during AST JSON deserialization", i);
            arr.push_back(readFieldFromObject(*elem));
        }
        return Field(std::move(arr));
    }
    if (field_type == "Tuple")
    {
        auto json_arr = obj.getArray("value");
        if (!json_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected JSON array for `value` of Tuple field during AST JSON deserialization");
        Tuple tup;
        tup.reserve(json_arr->size());
        for (unsigned int i = 0; i < json_arr->size(); ++i)
        {
            auto elem = json_arr->getObject(i);
            if (!elem)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in Tuple field during AST JSON deserialization", i);
            tup.push_back(readFieldFromObject(*elem));
        }
        return Field(std::move(tup));
    }
    if (field_type == "Map")
    {
        auto json_arr = obj.getArray("value");
        if (!json_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected JSON array for `value` of Map field during AST JSON deserialization");
        Map map;
        map.reserve(json_arr->size());
        for (unsigned int i = 0; i < json_arr->size(); ++i)
        {
            auto elem = json_arr->getObject(i);
            if (!elem)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in Map field during AST JSON deserialization", i);
            map.push_back(readFieldFromObject(*elem));
        }
        return Field(std::move(map));
    }

    /// For complex types, use Field::restoreFromDump.
    String dump_str = obj.getValue<String>("value");

    /// `Field::restoreFromDump` recursively parses nested `Array_`/`Tuple_`/`Map_` dumps
    /// without an internal depth limit, so a hostile JSON payload could trigger unbounded
    /// recursion even when the JSON object itself is shallow. Reject overly deep payloads
    /// against the same depth bound used for AST node construction.
    if (size_t max_depth = getJSONDeserializationMaxDepth();
        max_depth > 0 && computeFieldDumpNestingDepth(dump_str) > max_depth)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Field dump payload exceeds maximum AST depth limit ({}) during JSON AST deserialization",
            max_depth);

    return Field::restoreFromDump(dump_str);
}

}
