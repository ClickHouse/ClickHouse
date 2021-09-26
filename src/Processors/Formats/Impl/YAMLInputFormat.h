#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;

/** This class is used only to read YAML files for YAMLDictionarySource.
  */
class YAMLInputFormat : public IRowInputFormat
{
public:
    YAMLInputFormat(
	const Block & header_,
	ReadBuffer & in_,
	const Params & params_,
	const FormatSettings & format_settings_);

    String getName() const override { return "YAMLInputFormat"; }
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

private:
    void saveData(MutableColumns & columns, int id, int parent_id, String regexp, std::map<String, String> data);
    // This is used in readRow and Constructor
    const FormatSettings format_settings;
    size_t num_columns;
    std::vector<String> attrs;
    int spaces;
    std::map<int, int> parents;
    int current_id;
    std::map<String, String> data;
    String name;
    bool wasSet;
    
};

}

