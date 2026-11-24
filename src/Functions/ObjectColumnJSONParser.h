#pragma once

#include "config.h"

#if USE_SIMDJSON

#include <Core/ColumnWithTypeAndName.h>
#include <Formats/FormatSettings.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>


namespace DB
{

class ColumnObject;
class ColumnString;
class ISerialization;
class SerializationObject;
using SerializationPtr = std::shared_ptr<const ISerialization>;

/// The `JSONParser` interface of the JSONPath functions over the rows of a `JSON` column. An object is navigated on the paths of
/// the column; a value is printed as `CAST(json AS String)` prints it and parsed by `SimdJSONParser`.
class ObjectColumnJSONParser
{
public:
    class Array;
    class Object;

    class Element
    {
    public:
        Element() = default;

        [[nodiscard]] bool isObject() const { return is_sub_object || value.isObject(); }
        [[nodiscard]] bool isArray() const { return !is_sub_object && value.isArray(); }
        [[nodiscard]] bool isString() const { return !is_sub_object && value.isString(); }
        [[nodiscard]] std::string_view getString() const { return value.getString(); }
        [[nodiscard]] Array getArray() const;
        [[nodiscard]] Object getObject() const;
        [[nodiscard]] const Element & getElement() const { return *this; }

    private:
        friend class ObjectColumnJSONParser;
        friend class Array;
        friend class Object;
        friend class Formatter;

        Element(ObjectColumnJSONParser * parser_, std::string_view prefix_) : parser(parser_), prefix(prefix_), is_sub_object(true) { }
        Element(ObjectColumnJSONParser * parser_, SimdJSONParser::Element value_) : parser(parser_), value(value_) { }

        ObjectColumnJSONParser * parser = nullptr;
        /// Of a sub-object: its paths start with it; empty for the whole row, else ends with `.`.
        std::string_view prefix;
        SimdJSONParser::Element value;
        bool is_sub_object = false;
    };

    class Array
    {
    public:
        [[nodiscard]] size_t size() const { return array.size(); }
        Element operator[](size_t index) const { return Element(parser, array[index]); }

    private:
        friend class Element;

        Array(ObjectColumnJSONParser * parser_, SimdJSONParser::Array array_) : parser(parser_), array(array_) { }

        ObjectColumnJSONParser * parser;
        SimdJSONParser::Array array;
    };

    class Object
    {
    public:
        bool find(std::string_view key, Element & result) const;

    private:
        friend class Element;

        explicit Object(const Element & element_) : element(element_) { }

        Element element;
    };

    /// Appends an element the way `SimdJSONElementFormatter` appends the same element of `CAST(json AS String)`.
    class Formatter
    {
    public:
        explicit Formatter(PaddedPODArray<UInt8> & buffer_) : buffer(buffer_) { }
        void append(const Element & element);

    private:
        PaddedPODArray<UInt8> & buffer;
    };

    ObjectColumnJSONParser() = default;
    ObjectColumnJSONParser(const ObjectColumnJSONParser &) = delete;
    ObjectColumnJSONParser & operator=(const ObjectColumnJSONParser &) = delete;

    void prepare(const ColumnWithTypeAndName & json, const FormatSettings & format_settings_);

    Element getDocument(size_t row_)
    {
        row = row_;
        return Element(this, std::string_view{});
    }

private:
    struct TypedPath
    {
        const IColumn * column;
        const ISerialization * serialization;
    };

    bool findMember(std::string_view prefix, std::string_view key, Element & result);
    bool findValue(std::string_view value_path, Element & result);
    [[nodiscard]] bool hasPathsWithPrefix(std::string_view prefix) const;
    void appendSubObject(std::string_view prefix, PaddedPODArray<UInt8> & buffer);

    template <typename Write>
    SimdJSONParser::Element materializeValue(const void * source, size_t index, Write && write);

    ColumnPtr column_holder;
    const ColumnObject * column = nullptr;
    SerializationPtr serialization_holder;
    const SerializationObject * serialization = nullptr;
    FormatSettings format_settings;
    UnorderedMapWithMemoryTracking<std::string_view, TypedPath> typed_paths;
    const ColumnString * shared_data_paths = nullptr;
    const ColumnString * shared_data_values = nullptr;
    const PaddedPODArray<UInt64> * shared_data_offsets = nullptr;
    size_t row = 0;

    SetWithMemoryTracking<String, std::less<>> prefixes;
    String path;

    /// The value printed last, identified by its column (or shared data values) and its row (or index) there.
    const void * value_source = nullptr;
    size_t value_index = 0;
    String value_text;
    SimdJSONParser value_parser;
    SimdJSONParser::Element value;

    String sub_object_text;
    SimdJSONParser sub_object_parser;
};

inline ObjectColumnJSONParser::Array ObjectColumnJSONParser::Element::getArray() const
{
    return Array(parser, value.getArray());
}

inline ObjectColumnJSONParser::Object ObjectColumnJSONParser::Element::getObject() const
{
    return Object(*this);
}

}

#endif
