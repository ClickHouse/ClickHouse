#pragma once
#include <base/types.h>

namespace DB
{

enum class JsonEnumType : UInt8
{
    OBJECT,
    ARRAY,
    STRING,
    INT,
    UINT,
    DOUBLE,
    LITERAL_NULL,
    LITERAL_TRUE,
    LITERAL_FALSE,
    OPAQUE,
    ERROR
};

class JsonValue
{
public:
    JsonValue(JsonEnumType type_) : json_type(type_)
    {
    }

    JsonValue(JsonEnumType type_, Int64 val) : json_type(type_), int_value(val)
    {
    }

    JsonValue(JsonEnumType type_, double val) : json_type(type_), double_value(val)
    {
    }

    JsonValue(JsonEnumType type_, String val) : json_type(type_), string_value(val)
    {
    }

    JsonValue(JsonEnumType type_, const char* data_, size_t bytes):
        json_type(type_),
        data(data_),
        data_length(bytes)
    {
        if (type_ == JsonEnumType::STRING)
        {
            string_value.assign(data_, bytes);
        }
    }

    JsonValue(int field_type_, const char* data_, size_t bytes):
        json_type(JsonEnumType::OPAQUE),
        field_type(field_type_),
        data(data_),
        data_length(bytes)
    {
    }

    JsonValue(JsonEnumType type_, const char* data_, size_t bytes, size_t element_count_, bool large_):
        json_type(type_),
        data(data_),
        data_length(bytes),
        element_count(element_count_),
        large(large_)
    {
    }

    void toJsonString(String & out);
private:
    void parseOpaque(String & out);
    JsonValue getJsonValueForElement(size_t pos);
    String getObjectKey(size_t pos);

    JsonEnumType  json_type;
    int           field_type;
    const char *  data;
    size_t        data_length;
    size_t        element_count;
    String        string_value;
    Int64         int_value;
    double        double_value;
    bool          large;
};

JsonValue parseBinary(const String & in);

}
