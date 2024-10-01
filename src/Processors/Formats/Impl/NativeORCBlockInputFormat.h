#pragma once
#include "config.h"

#if USE_ORC
#    include <Formats/FormatSettings.h>
#    include <IO/ReadBufferFromString.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <Storages/MergeTree/KeyCondition.h>
#    include <boost/algorithm/string.hpp>
#    include <orc/OrcFile.hh>

namespace DB
{

class ORCInputStream : public orc::InputStream
{
public:
    ORCInputStream(SeekableReadBuffer & in_, size_t file_size_);

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void * buf, uint64_t length, uint64_t offset) override;
    const std::string & getName() const override { return name; }

protected:
    SeekableReadBuffer & in;
    size_t file_size;
    std::string name = "ORCInputStream";
};

class ORCInputStreamFromString : public ReadBufferFromOwnString, public ORCInputStream
{
public:
    template <typename S>
    ORCInputStreamFromString(S && s_, size_t file_size_)
        : ReadBufferFromOwnString(std::forward<S>(s_)), ORCInputStream(dynamic_cast<SeekableReadBuffer &>(*this), file_size_)
    {
    }
};

std::unique_ptr<orc::InputStream> asORCInputStream(ReadBuffer & in, const FormatSettings & settings, std::atomic<int> & is_cancelled);

// Reads the whole file into a memory buffer, owned by the returned RandomAccessFile.
std::unique_ptr<orc::InputStream> asORCInputStreamLoadIntoMemory(ReadBuffer & in, std::atomic<int> & is_cancelled);

std::unique_ptr<orc::SearchArgument> buildORCSearchArgument(
    const KeyCondition & key_condition, const Block & header, const orc::Type & schema, const FormatSettings & format_settings);

class ORCColumnToCHColumn;
class NativeORCBlockInputFormat : public IInputFormat
{
public:
    NativeORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues & getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk read() override;

    void onCancel() override { is_stopped = 1; }

private:
    void prepareFileReader();
    bool prepareStripeReader();

    std::unique_ptr<orc::Reader> file_reader;
    std::unique_ptr<orc::RowReader> stripe_reader;
    std::unique_ptr<ORCColumnToCHColumn> orc_column_to_ch_column;

    std::shared_ptr<orc::SearchArgument> sarg;

    // indices of columns to read from ORC file
    std::list<UInt64> include_indices;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_stripes;

    int total_stripes = 0;
    int current_stripe = -1;
    std::unique_ptr<orc::StripeInformation> current_stripe_info;

    std::atomic<int> is_stopped{0};
};

class NativeORCSchemaReader : public ISchemaReader
{
public:
    NativeORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

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

    ORCColumnToCHColumn(const Block & header_, bool allow_missing_columns_, bool null_as_default_, bool case_insensitive_matching_ = false);

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
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;
};
}
#endif
