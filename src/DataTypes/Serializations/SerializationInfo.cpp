#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void SerializationInfo::add(const Block & block)
{
    number_of_rows += block.rows();
    for (const auto & elem : block)
    {
        non_default_values[elem.name] = elem.column->getNumberOfNonDefaultValues();
        for (const auto & subname : elem.type->getSubcolumnNames())
        {
            auto subcolumn = elem.type->getSubcolumn(subname, *elem.column);
            auto full_name = Nested::concatenateName(elem.name, subname);
            non_default_values[full_name] += subcolumn->getNumberOfNonDefaultValues();
        }
    }
}

void SerializationInfo::add(const SerializationInfo & other)
{
    number_of_rows += other.number_of_rows;
    for (const auto & [name, num] : other.non_default_values)
        non_default_values[name] += num;
}

size_t SerializationInfo::getNumberOfNonDefaultValues(const String & column_name) const
{
    auto it = non_default_values.find(column_name);
    if (it == non_default_values.end())
        return 0;
    return it->second;
}

namespace
{

constexpr auto KEY_NUMBER_OF_ROWS = "number_of_rows";
constexpr auto KEY_NUMBER_OF_NON_DEFAULT_VALUES = "number_of_non_default_values";
constexpr auto KEY_NUMBER = "number";
constexpr auto KEY_NAME = "name";
constexpr auto KEY_VERSION = "version";

}

void SerializationInfo::fromJSON(const String & json_str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    if (object->has(KEY_NUMBER_OF_ROWS))
        number_of_rows = object->getValue<size_t>(KEY_NUMBER_OF_ROWS);
    
    if (object->has(KEY_NUMBER_OF_NON_DEFAULT_VALUES))
    {
        auto array = object->getArray(KEY_NUMBER_OF_NON_DEFAULT_VALUES);
        for (const auto & elem : *array)
        {
            auto elem_object = elem.extract<Poco::JSON::Object::Ptr>();
            if (!elem_object->has(KEY_NUMBER) || !elem_object->has(KEY_NAME))
                throw Exception(ErrorCodes::LOGICAL_ERROR, 
                    "Missed field 'name' or 'number' in SerializationInfo of columns");

            auto name = elem_object->getValue<String>(KEY_NAME);
            auto number = elem_object->getValue<size_t>(KEY_NUMBER);
            non_default_values[name] = number;
        }
    }
}

String SerializationInfo::toJSON() const
{
    Poco::JSON::Object info;
    info.set(KEY_VERSION, version);
    info.set(KEY_NUMBER_OF_ROWS, number_of_rows);
    
    Poco::JSON::Array column_infos;
    for (const auto & [name, num] : non_default_values)
    {
        Poco::JSON::Object column_info;
        column_info.set(KEY_NAME, name);
        column_info.set(KEY_NUMBER, num);
        column_infos.add(std::move(column_info));
    }

    info.set(KEY_NUMBER_OF_NON_DEFAULT_VALUES, std::move(column_infos));

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(info, oss);

    return oss.str();
}

void SerializationInfo::read(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    fromJSON(json_str);
}

void SerializationInfo::write(WriteBuffer & out) const
{
    writeString(toJSON(), out);
}

}
