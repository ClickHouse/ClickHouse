#pragma once
#include "config_formats.h"
#if USE_ORC

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

#include <arrow/adapters/orc/adapter.h>

namespace arrow::adapters::orc
{
    class ORCFileReader;
}

namespace DB
{

class ArrowColumnToCHColumn;

class ORCBlockInputFormat : public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues & getMissingValues() const override;

protected:
    Chunk generate() override;

    void onCancel() override
    {
        is_stopped = 1;
    }

private:

    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;
    std::vector<String> include_column_names;

    std::vector<size_t> missing_columns;
    BlockMissingValues block_missing_values;

    const FormatSettings format_settings;

    void prepareReader();

    std::atomic<int> is_stopped{0};
};

class ORCSchemaReader : public ISchemaReader
{
public:
    ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}
#endif
