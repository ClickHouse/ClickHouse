#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    return Field::restoreFromDump(dump_str);
}

}
