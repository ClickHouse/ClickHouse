#pragma once

#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;

/** A stream for reading data in JSONCompactEachRow and JSONCompactEachRowWithNamesAndTypes formats
*/
class JSONCompactEachRowRowInputFormat : public IRowInputFormat
{
public:
    JSONCompactEachRowRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_, bool with_names_);

    String getName() const override { return "JSONCompactEachRowRowInputFormat"; }


    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;


private:
    void addInputColumn(const String & column_name);
    void skipEndOfLine();
    void readField(size_t index, MutableColumns & columns);

    const FormatSettings format_settings;

    using IndexesMap = std::unordered_map<String, size_t>;
    IndexesMap column_indexes_by_names;

    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    DataTypes data_types;
    std::vector<UInt8> read_columns;

    /// This is for the correct exceptions in skipping unknown fields.
    std::vector<StringRef> names_of_columns;

    bool have_always_default_columns = false;

    bool with_names;
};

}
