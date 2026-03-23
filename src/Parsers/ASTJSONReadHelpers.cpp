#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

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
        Array arr;
        auto json_arr = obj.getArray("value");
        if (json_arr)
            for (unsigned int i = 0; i < json_arr->size(); ++i)
                arr.push_back(readFieldFromObject(*json_arr->getObject(i)));
        return Field(std::move(arr));
    }
    if (field_type == "Tuple")
    {
        Tuple tup;
        auto json_arr = obj.getArray("value");
        if (json_arr)
            for (unsigned int i = 0; i < json_arr->size(); ++i)
                tup.push_back(readFieldFromObject(*json_arr->getObject(i)));
        return Field(std::move(tup));
    }
    if (field_type == "Map")
    {
        Map map;
        auto json_arr = obj.getArray("value");
        if (json_arr)
            for (unsigned int i = 0; i < json_arr->size(); ++i)
                map.push_back(readFieldFromObject(*json_arr->getObject(i)));
        return Field(std::move(map));
    }

    /// For complex types, use Field::restoreFromDump.
    String dump_str = obj.getValue<String>("value");
    return Field::restoreFromDump(dump_str);
}

}
