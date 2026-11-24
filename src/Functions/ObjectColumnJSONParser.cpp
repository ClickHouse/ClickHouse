#include <Functions/ObjectColumnJSONParser.h>

#if USE_SIMDJSON

#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

template <typename Write>
SimdJSONParser::Element printAndParse(String & text, SimdJSONParser & parser, Write && write)
{
    {
        WriteBufferFromVector<String> out(text);
        write(out);
        out.finalize();
    }
    size_t size = text.size();
    text.resize(size + simdjson::SIMDJSON_PADDING);

    SimdJSONParser::Element element;
    if (!parser.parse(std::string_view(text.data(), size), simdjson::SIMDJSON_PADDING, element))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot parse a value of a JSON column as JSON: {}",
            std::string_view(text.data(), std::min<size_t>(size, 1000)));
    return element;
}

}

void ObjectColumnJSONParser::prepare(const ColumnWithTypeAndName & json, const FormatSettings & format_settings_)
{
    column_holder = json.column->convertToFullColumnIfConst();
    column = &assert_cast<const ColumnObject &>(*column_holder);
    serialization_holder = json.type->getDefaultSerialization();
    serialization = &dynamic_cast<const SerializationObject &>(*serialization_holder);
    format_settings = format_settings_;

    typed_paths.clear();
    const auto & typed_paths_serializations = serialization->getTypedPathsSerializations();
    for (const auto & [typed_path, typed_column] : column->getTypedPaths())
        typed_paths[typed_path] = {typed_column.get(), typed_paths_serializations.at(typed_path).get()};

    std::tie(shared_data_paths, shared_data_values) = column->getSharedDataPathsAndValues();
    shared_data_offsets = &column->getSharedDataOffsets();
    value_source = nullptr;
}

template <typename Write>
SimdJSONParser::Element ObjectColumnJSONParser::materializeValue(const void * source, size_t index, Write && write)
{
    if (source != value_source || index != value_index)
    {
        value_source = nullptr;
        value = printAndParse(value_text, value_parser, write);
        value_source = source;
        value_index = index;
    }
    return value;
}

bool ObjectColumnJSONParser::findValue(std::string_view value_path, Element & result)
{
    if (auto it = typed_paths.find(value_path); it != typed_paths.end())
    {
        const auto & [typed_column, typed_serialization] = it->second;
        if (format_settings.json.type_json_skip_null_typed_paths && typed_column->isNullAt(row))
            return false;
        result = Element(this, materializeValue(typed_column, row, [&](WriteBuffer & out)
            { typed_serialization->serializeTextJSON(*typed_column, row, out, format_settings); }));
        return true;
    }

    const auto & dynamic_paths = column->getDynamicPathsPtrs();
    if (auto it = dynamic_paths.find(value_path); it != dynamic_paths.end() && !it->second->isNullAt(row))
    {
        const auto * dynamic_column = it->second;
        result = Element(this, materializeValue(dynamic_column, row, [&](WriteBuffer & out)
            { serialization->getDynamicPathSerialization()->serializeTextJSON(*dynamic_column, row, out, format_settings); }));
        return true;
    }

    size_t end = (*shared_data_offsets)[row];
    size_t index = ColumnObject::findPathLowerBoundInSharedData(
        value_path, *shared_data_paths, (*shared_data_offsets)[static_cast<ssize_t>(row) - 1], end);
    if (index == end || shared_data_paths->getDataAt(index) != value_path)
        return false;
    result = Element(this, materializeValue(shared_data_values, index, [&](WriteBuffer & out)
        { SerializationDynamic::serializeBinaryEncodedValueTextJSON(shared_data_values->getDataAt(index), out, format_settings, false, 0); }));
    return true;
}

bool ObjectColumnJSONParser::hasPathsWithPrefix(std::string_view prefix) const
{
    for (const auto & [typed_path, typed] : typed_paths)
        if (typed_path.starts_with(prefix) && !(format_settings.json.type_json_skip_null_typed_paths && typed.column->isNullAt(row)))
            return true;

    const auto & sorted_dynamic_paths = column->getSortedDynamicPaths();
    const auto & dynamic_paths = column->getDynamicPathsPtrs();
    for (auto it = sorted_dynamic_paths.lower_bound(prefix); it != sorted_dynamic_paths.end() && it->starts_with(prefix); ++it)
        if (!dynamic_paths.find(*it)->second->isNullAt(row))
            return true;

    size_t end = (*shared_data_offsets)[row];
    size_t index = ColumnObject::findPathLowerBoundInSharedData(
        prefix, *shared_data_paths, (*shared_data_offsets)[static_cast<ssize_t>(row) - 1], end);
    return index != end && shared_data_paths->getDataAt(index).starts_with(prefix);
}

/// A key is printed as its path component, unescaped with `json_type_escape_dots_in_keys`. Of a value at path `P` and paths
/// under `P.`, the value is printed first, so it is the one the JSON text finds.
bool ObjectColumnJSONParser::findMember(std::string_view prefix, std::string_view key, Element & result)
{
    path.assign(prefix);
    if (format_settings.json.json_type_escape_dots_in_keys)
    {
        String component = escapeDotInJSONKey(String(key));
        if (unescapeDotInJSONKey(component) != key)
            return false;
        path += component;
    }
    else
    {
        if (key.find('.') != std::string_view::npos)
            return false;
        path += key;
    }

    if (findValue(path, result))
        return true;

    path += '.';
    if (!hasPathsWithPrefix(path))
        return false;

    auto it = prefixes.find(path);
    if (it == prefixes.end())
        it = prefixes.emplace(path).first;
    result = Element(this, std::string_view(*it));
    return true;
}

void ObjectColumnJSONParser::appendSubObject(std::string_view prefix, PaddedPODArray<UInt8> & buffer)
{
    auto element = printAndParse(sub_object_text, sub_object_parser, [&](WriteBuffer & out)
        { serialization->serializeTextJSONSubObject(*column, row, prefix, out, format_settings); });
    SimdJSONElementFormatter(buffer).append(element.getElement());
}

bool ObjectColumnJSONParser::Object::find(std::string_view key, Element & result) const
{
    if (element.is_sub_object)
        return element.parser->findMember(element.prefix, key, result);

    SimdJSONParser::Element found;
    if (!element.value.getObject().find(key, found))
        return false;
    result = Element(element.parser, found);
    return true;
}

void ObjectColumnJSONParser::Formatter::append(const Element & element)
{
    if (element.is_sub_object)
        element.parser->appendSubObject(element.prefix, buffer);
    else
        SimdJSONElementFormatter(buffer).append(element.value.getElement());
}

}

#endif
