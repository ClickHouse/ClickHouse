#include "config.h"

#if USE_YTSAURUS
#include <Processors/Sources/YTsaurusSource.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>
#include <Processors/Sources/NullSource.h>
#include <optional>
#include <cstddef>
#include <memory>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace YTsaurusSetting
{
    extern const YTsaurusSettingsBool check_table_schema;
    extern const YTsaurusSettingsBool skip_unknown_columns;
    extern const YTsaurusSettingsBool force_read_table;
    extern const YTsaurusSettingsBool use_lock;
    extern const YTsaurusSettingsMilliseconds transaction_timeout_ms;
    extern const YTsaurusSettingsUInt64 min_rows_for_spawn_stream;
    extern const YTsaurusSettingsUInt64 max_streams;
    extern const YTsaurusSettingsUInt64 lookup_max_rows_per_query;
}

YTsaurusTableSourceStaticTable::YTsaurusTableSourceStaticTable(
    YTsaurusClientPtr client_, const String & cypress_path_, std::pair<size_t, size_t> rows_range_, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_)
    : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
    , client(std::move(client_))
    , cypress_path(cypress_path_)
    , rows_range(std::move(rows_range_))
    , source_options(source_options_)
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , json_row_format(nullptr)
{
}

Chunk YTsaurusTableSourceStaticTable::generate()
{
    if (!json_row_format)
    {
        FormatSettings format_settings{.skip_unknown_fields = source_options.settings[YTsaurusSetting::skip_unknown_columns]};
        format_settings.json.read_map_as_array_of_tuples = true;
        /// YTsaurus stores `timestamp`/`timestamp64` as raw ticks (microseconds for the mapped `DateTime64(6)`), not as
        /// Unix seconds, so an unquoted number must be read as the raw underlying value.
        format_settings.read_datetime_number_as_raw_value = true;
        read_buffer = client->readTable(cypress_path, rows_range);

        json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
            *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size_rows = max_block_size}), format_settings, false);

    }
    return json_row_format->read();
}

YTsaurusTableSourceDynamicTableSelect::YTsaurusTableSourceDynamicTableSelect(
    YTsaurusClientPtr client_,
    const String & cypress_path_,
    const SharedHeader & sample_block_,
    const UInt64 & max_block_size_,
    bool format_skip_unknown_columns_,
    std::optional<String> select_rows_columns_,
    YTsaurusTableLockPtr table_lock_)
    : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
    , client(std::move(client_))
    , cypress_path(cypress_path_)
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , format_settings({.skip_unknown_fields = format_skip_unknown_columns_})
    , select_rows_columns(std::move(select_rows_columns_))
    , table_lock(table_lock_)
{
    format_settings.json.read_map_as_array_of_tuples = true;
    /// YTsaurus stores `timestamp`/`timestamp64` as raw ticks (microseconds for the mapped `DateTime64(6)`), not as
    /// Unix seconds, so an unquoted number must be read as the raw underlying value.
    format_settings.read_datetime_number_as_raw_value = true;
}

Chunk YTsaurusTableSourceDynamicTableSelect::generate()
{
    if (!json_row_format)
    {
        if (select_rows_columns)
            read_buffer = client->selectRows(cypress_path, *select_rows_columns);
        else
            read_buffer = client->selectRows(cypress_path, sample_block->getColumnsWithTypeAndName());

        json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
            *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size_rows = max_block_size}), format_settings, false);
    }

    return json_row_format->read();
}

YTsaurusTableSourceDynamicTableLookup::YTsaurusTableSourceDynamicTableLookup(
    YTsaurusClientPtr client_,
    const String & cypress_path_,
    const SharedHeader & sample_block_,
    const UInt64 & max_block_size_,
    bool format_skip_unknown_columns_,
    Block lookup_input_block_,
    size_t lookup_max_rows_per_query_,
    ThrottlerPtr lookup_throttler_,
    YTsaurusTableLockPtr table_lock_)
    : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
    , client(std::move(client_))
    , cypress_path(cypress_path_)
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , format_settings({.skip_unknown_fields = format_skip_unknown_columns_})
    , lookup_input_block(std::move(lookup_input_block_))
    , lookup_max_rows_per_query(lookup_max_rows_per_query_)
    , lookup_throttler(std::move(lookup_throttler_))
    , table_lock(table_lock_)
{
    format_settings.json.read_map_as_array_of_tuples = true;
    /// YTsaurus stores `timestamp`/`timestamp64` as raw ticks (microseconds for the mapped `DateTime64(6)`), not as
    /// Unix seconds, so an unquoted number must be read as the raw underlying value.
    format_settings.read_datetime_number_as_raw_value = true;
}

Chunk YTsaurusTableSourceDynamicTableLookup::generate()
{
    const size_t total_rows = lookup_input_block.rows();
    while (true)
    {
        if (!json_row_format)
        {
            const size_t chunk_rows = lookupChunkRows(total_rows, next_row, lookup_max_rows_per_query);
            if (!chunk_rows)
                return {};

            /// The payload of a single request is cut out lazily: `lookupRows` copies the (shallow) block into its
            /// request callback, so the slice does not have to outlive this call.
            const Block lookup_input_chunk = chunk_rows == total_rows
                ? lookup_input_block
                : lookup_input_block.cloneWithCutColumns(next_row, chunk_rows);
            read_buffer = client->lookupRows(cypress_path, lookup_input_chunk, lookup_throttler);
            next_row += chunk_rows;
            json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
                *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size_rows = max_block_size}), format_settings, false);
        }

        auto chunk = json_row_format->read();
        if (chunk.hasRows())
            return chunk;

        /// The response for the current lookup request is exhausted, proceed to the next one.
        json_row_format.reset();
        read_buffer.reset();
    }
}


namespace
{

Pipe createPipeForStaticTable(
    YTsaurusClientPtr client,
    const String & cypress_path,
    const String & table_cypress_path,
    const YTsaurusTableSourceOptions & source_options,
    const SharedHeader & sample_block,
    UInt64 max_block_size,
    UInt64 max_streams)
{
    const YTsaurusSettings & settings = source_options.settings;
    auto rows_count = client->getTableNumberOfRows(cypress_path);

    size_t max_streams_allowed = max_streams ? std::min<size_t>(settings[YTsaurusSetting::max_streams], max_streams)
                                              : settings[YTsaurusSetting::max_streams];

    size_t pipes_num = max_streams_allowed;
    auto min_rows_for_spawn_stream = settings[YTsaurusSetting::min_rows_for_spawn_stream];
    if (min_rows_for_spawn_stream)
        pipes_num = std::min(max_streams_allowed, std::max<size_t>(1u, rows_count / min_rows_for_spawn_stream));

    size_t rows_batch_count = rows_count / pipes_num;
    Pipes pipes;

    LOG_DEBUG(::getLogger("YTsaurusSourceFactory"),
        "Will read static table {} with {} streams and rows {} in each",
        table_cypress_path, pipes_num, rows_batch_count);

    for (size_t i = 0; i < pipes_num; ++i)
    {
        size_t row_from = i * rows_batch_count;
        size_t row_to = (i + 1 == pipes_num) ? rows_count : (i + 1) * rows_batch_count;
        auto client_for_source = std::make_shared<YTsaurusClient>(*client);
        pipes.emplace_back(std::make_shared<YTsaurusTableSourceStaticTable>(
            client_for_source,
            cypress_path,
            std::make_pair(row_from, row_to),
            source_options,
            sample_block,
            max_block_size));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    pipe.resize(pipes_num);
    return pipe;
}

Pipe createPipeForDynamicTable(
    YTsaurusClientPtr client,
    const String & cypress_path,
    YTsaurusTableSourceOptions & source_options,
    const SharedHeader & sample_block,
    UInt64 max_block_size)
{
    bool use_lookups = !source_options.settings[YTsaurusSetting::force_read_table]
                       && source_options.lookup_input_block.has_value();
    bool skip_unknown_columns = source_options.settings[YTsaurusSetting::skip_unknown_columns];

    if (use_lookups)
    {
        /// An empty key set must stay a no-op: no `lookup_rows` request is issued for it, so it consumes no
        /// throttler token either.
        const size_t lookup_rows = source_options.lookup_input_block->rows();
        if (!lookup_rows)
            return Pipe(std::make_shared<NullSource>(sample_block));

        const size_t lookup_max_rows_per_query = source_options.settings[YTsaurusSetting::lookup_max_rows_per_query];

        LOG_DEBUG(::getLogger("YTsaurusSourceFactory"),
        "Will read dynamic table {} in lookup mode with {} rows split by at most {} rows per request",
            cypress_path, lookup_rows, lookup_max_rows_per_query);

        /// A single source issues the lookup requests one by one, cutting each request payload out of the whole
        /// lookup input only right before the request: materializing one source (or one `Block`) per chunk would
        /// scale with the number of chunks when `lookup_max_rows_per_query` is small relative to the number of
        /// requested keys.
        return Pipe(std::make_shared<YTsaurusTableSourceDynamicTableLookup>(
            client,
            cypress_path,
            sample_block,
            max_block_size,
            skip_unknown_columns,
            std::move(*source_options.lookup_input_block),
            lookup_max_rows_per_query,
            source_options.lookup_throttler,
            source_options.table_lock));
    }
    else
    {
        LOG_DEBUG(::getLogger("YTsaurusSourceFactory"),
        "Will read dynamic table {} with select mode", cypress_path);

        return Pipe(std::make_shared<YTsaurusTableSourceDynamicTableSelect>(
            client,
            cypress_path,
            sample_block,
            max_block_size,
            skip_unknown_columns,
            source_options.select_rows_columns,
            source_options.table_lock));
    }
}

}

Pipe YTsaurusSourceFactory::createPipe(
    YTsaurusClientPtr client,
    const String & table_cypress_path,
    YTsaurusTableSourceOptions source_options,
    const SharedHeader & sample_block,
    UInt64 max_block_size,
    UInt64 max_streams)
{
    if (table_cypress_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cypress path are empty for ytsarurus source factory.");

    const YTsaurusSettings & settings = source_options.settings;
    String cypress_path;

    if (settings[YTsaurusSetting::use_lock])
    {
        source_options.table_lock = std::make_shared<YTsaurusTableLock>(
            client, table_cypress_path, settings[YTsaurusSetting::transaction_timeout_ms].totalMilliseconds());
        cypress_path = source_options.table_lock->getNodePath();
    }
    else
    {
        cypress_path = table_cypress_path;
    }

    if (settings[YTsaurusSetting::check_table_schema])
    {
        String reason;
        if (!client->checkSchemaCompatibility(cypress_path, sample_block, reason, source_options.check_types_allow_nullable))
            throw Exception(ErrorCodes::INCORRECT_DATA, "ClickHouse table schema doesn't match with yt table. Reason: {}", reason);
    }

    auto yt_node_type = client->getNodeType(cypress_path);

    if (yt_node_type == YTsaurusNodeType::STATIC_TABLE)
        return createPipeForStaticTable(client, cypress_path, table_cypress_path, source_options, sample_block, max_block_size, max_streams);    else if (yt_node_type == YTsaurusNodeType::DYNAMIC_TABLE)
        return createPipeForDynamicTable(client, cypress_path, source_options, sample_block, max_block_size);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Node {} has unsupported type.", cypress_path);
}

}
#endif
