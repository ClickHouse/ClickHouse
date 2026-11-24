#pragma once

#include "config.h"

#if USE_ORC

#include <Core/BlockMissingValues.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFilterInfo.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <boost/algorithm/string.hpp>
#include <Processors/Formats/Impl/ORC/Reader.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB
{

class KeyCondition;

class ORCInputStream : public ORC::InputStream
{
public:
    ORCInputStream(SeekableReadBuffer & in_, size_t file_size_, bool use_prefetch);

    UInt64 getLength() const override;
    void read(void * buf, UInt64 length, UInt64 offset) override;
    std::future<void> readAsync(void * buf, UInt64 length, UInt64 offset) override;
    const std::string & getName() const override { return name; }

protected:
    SeekableReadBuffer & in;
    size_t file_size;
    /// Use offset-based reads (ReadBuffer::readBigAt) instead of seek+read; needed for ORC tail.
    bool use_offset_based_read;
    /// Async wrapper only when caller enabled prefetch and the buffer supports read-at.
    bool use_async_prefetch;
    ThreadPoolCallbackRunnerUnsafe<void> async_runner;

    std::string name = "ORCInputStream";
};

class ORCInputStreamFromString : public ReadBufferFromOwnString, public ORCInputStream
{
public:
    template <typename S>
    ORCInputStreamFromString(S && s_, size_t file_size_)
        : ReadBufferFromOwnString(std::forward<S>(s_)), ORCInputStream(dynamic_cast<SeekableReadBuffer &>(*this), file_size_, false)
    {
    }
};

std::unique_ptr<ORC::InputStream>
asORCInputStream(ReadBuffer & in, const FormatSettings & settings, bool use_prefetch, std::atomic<int> & is_cancelled);

// Reads the whole file into a memory buffer, owned by the returned RandomAccessFile.
std::unique_ptr<ORC::InputStream> asORCInputStreamLoadIntoMemory(ReadBuffer & in, std::atomic<int> & is_cancelled);

/// Creates an ORC file reader; throws INCORRECT_DATA if the root type of the file is not a struct.
std::unique_ptr<ORC::Reader> createORCReader(std::unique_ptr<ORC::InputStream> stream, const ORC::ReaderOptions & options);

std::unique_ptr<ORC::SearchArgument> buildORCSearchArgument(
    const KeyCondition & key_condition, const Block & header, const ORC::Type & schema, const FormatSettings & format_settings);

class ORCColumnToCHColumn;
class NativeORCBlockInputFormat final : public IInputFormat
{
public:
    NativeORCBlockInputFormat(
        ReadBuffer & in_,
        SharedHeader header_,
        const FormatSettings & format_settings_,
        bool use_prefetch_,
        size_t min_bytes_for_seek_,
        FormatFilterInfoPtr format_filter_info_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk read() override;

    void onCancel() noexcept override { is_stopped = 1; }

private:
    static std::vector<int> calculateSelectedStripes(int num_stripes, const std::unordered_set<int> & skip_stripes);

    void prepareFileReader();
    bool prepareStripeReader();
    void prefetchStripes();

    std::unique_ptr<ORC::Reader> file_reader;
    std::unique_ptr<ORC::RowReader> stripe_reader;
    std::unique_ptr<ORCColumnToCHColumn> orc_column_to_ch_column;

    std::shared_ptr<ORC::SearchArgument> sargs;

    // indices of columns to read from ORC file
    std::vector<UInt64> include_indices;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_stripes;
    const bool use_prefetch;
    const size_t min_bytes_for_seek;
    FormatFilterInfoPtr format_filter_info;

    std::vector<int> selected_stripes;
    size_t read_iterator{};
    size_t prefetch_iterator{};

    const ORC::Proto::StripeInformation * current_stripe_info = nullptr;

    std::atomic<int> is_stopped{0};
};

class NativeORCSchemaReader final : public ISchemaReader
{
public:
    NativeORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    const FormatSettings format_settings;
    std::unique_ptr<ORC::Reader> file_reader;
    std::atomic<int> is_stopped{0};
    bool initialized = false;
};

class ORCColumnToCHColumn
{
public:
    using ORCColumnPtr = const ORC::ColumnVectorBatch *;
    using ORCTypePtr = const ORC::Type *;
    using ORCColumnWithType = std::pair<ORCColumnPtr, ORCTypePtr>;
    using NameToColumnPtr = std::unordered_map<std::string, ORCColumnWithType>;

    ORCColumnToCHColumn(
        const Block & header_,
        bool allow_missing_columns_,
        bool null_as_default_,
        bool case_insensitive_matching_ = false,
        bool dictionary_as_low_cardinality_ = false,
        FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior_ = FormatSettings::DateTimeOverflowBehavior::Ignore);

    void orcTableToCHChunk(
        Chunk & res,
        const ORC::Type * schema,
        const ORC::ColumnVectorBatch * table,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr);

    void orcColumnsToCHChunk(
        Chunk & res,
        NameToColumnPtr & name_to_column_ptr,
        const Names & orc_field_names,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr);

private:
    ColumnWithTypeAndName readColumnFromORCColumn(
        const ORC::ColumnVectorBatch * orc_column,
        const ORC::Type * orc_type,
        const std::string & column_name,
        bool inside_nullable,
        DataTypePtr type_hint = nullptr) const;

    const Block & header;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;
    bool dictionary_as_low_cardinality;
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior;
};
}
#endif
