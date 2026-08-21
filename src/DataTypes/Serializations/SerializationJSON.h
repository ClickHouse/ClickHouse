#pragma once

#include <DataTypes/Serializations/SerializationObject.h>
#include <Formats/JSONExtractTree.h>
#include <Common/ObjectPool.h>

namespace DB
{

/// Class for text serialization/deserialization of the JSON data type.
template <typename Parser>
class SerializationJSON : public SerializationObject
{
public:
    SerializationJSON(
        const std::unordered_map<String, DataTypePtr> & typed_paths_types_,
        const std::unordered_set<String> & paths_to_skip_,
        const std::vector<String> & path_regexps_to_skip_,
        const DataTypePtr & dynamic_type_,
        std::unique_ptr<JSONExtractTreeNode<Parser>> json_extract_tree_);

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeObject(IColumn & column, std::string_view object, const FormatSettings & settings) const override;

private:
    void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, bool pretty = false, size_t indent = 0) const;

    std::unique_ptr<JSONExtractTreeNode<Parser>> json_extract_tree;
    /// Pool of parser objects to make SerializationJSON thread safe.
    mutable SimpleObjectPool<Parser> parsers_pool;
};

}
