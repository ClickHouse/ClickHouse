#pragma once
#include "config.h"

#if USE_ORC
#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <orc/OrcFile.hh>

namespace DB
{

class ORCInputStream : public orc::InputStream
{
public:
    explicit ORCInputStream(SeekableReadBuffer & in_);

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void * buf, uint64_t length, uint64_t offset) override;
    const std::string & getName() const override { return name; }

private:
    SeekableReadBuffer & in;
    std::string name = "ORCInputStream";
};

std::unique_ptr<orc::InputStream> asORCInputStream(ReadBuffer & in);


class ORCColumnToCHColumn;
class ORCBlockInputFormat : public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues & getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk generate() override;

    void onCancel() override { is_stopped = 1; }

private:
    void prepareFileReader();
    bool prepareStripeReader();

    std::unique_ptr<orc::Reader> file_reader;
    std::unique_ptr<orc::RowReader> stripe_reader;
    std::unique_ptr<ORCColumnToCHColumn> orc_column_to_ch_column;
    std::unique_ptr<orc::ColumnVectorBatch> batch;

    // indices of columns to read from ORC file
    std::list<UInt64> include_indices;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk;

    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_stripes;

    int total_stripes = 0;
    int current_stripe = -1;
    std::unique_ptr<orc::StripeInformation> current_stripe_info;

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

class ORCColumnToCHColumn
{
public:
    using ORCColumnPtr = const orc::ColumnVectorBatch *;
    using ORCTypePtr = const orc::Type *;
    using ORCColumnWithType = std::pair<ORCColumnPtr, ORCTypePtr>;
    using NameToColumnPtr = std::unordered_map<std::string, ORCColumnWithType>;

    ORCColumnToCHColumn(
        const Block & header_,
        bool import_nested_,
        bool allow_missing_columns_,
        bool null_as_default_,
        bool case_insensitive_matching_ = false);

    void orcTableToCHChunk(
        Chunk & res,
        const orc::Type * schema,
        const orc::ColumnVectorBatch * table,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr);

    void orcColumnsToCHChunk(
        Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

private:
    const Block & header;
    bool import_nested;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;
};
}
#endif
