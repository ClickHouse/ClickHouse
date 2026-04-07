#include "config.h"

#if USE_YTSAURUS
#include <Processors/Sources/YTsaurusSource.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>

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
}

YTsaurusTableSourceStaticTable::YTsaurusTableSourceStaticTable(
    YTsaurusClientPtr client_, const String & cypress_path_, std::pair<size_t, size_t> rows_range_, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_)
    : ISource(sample_block_)
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
        read_buffer = client->readTable(cypress_path, rows_range);

        json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
            *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size_rows = max_block_size}), format_settings, false);

    }
    return json_row_format->read();
}

YTsaurusTableSourceDynamicTable::YTsaurusTableSourceDynamicTable(
    YTsaurusClientPtr client_, const String & cypress_path, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_)
    : ISource(sample_block_)
    , client(std::move(client_))
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , format_settings({.skip_unknown_fields = source_options_.settings[YTsaurusSetting::skip_unknown_columns]})
    , use_lookups(!source_options_.settings[YTsaurusSetting::force_read_table] && source_options_.lookup_input_block)
    , table_lock(source_options_.table_lock)
{
    if (use_lookups)
    {
        read_buffer = client->lookupRows(cypress_path, *source_options_.lookup_input_block);
    }
    else
    {
        if (source_options_.select_rows_columns)
            read_buffer = client->selectRows(cypress_path, *source_options_.select_rows_columns);
        else
            read_buffer = client->selectRows(cypress_path, sample_block->getColumnsWithTypeAndName());
    }

    format_settings.json.read_map_as_array_of_tuples = true;
    json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
        *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size_rows = max_block_size}), format_settings, false);
}

Pipe YTsaurusSourceFactory::createPipe(
      YTsaurusClientPtr client
    , const String & table_cypress_path
    , YTsaurusTableSourceOptions source_options
    , const SharedHeader & sample_block
    , const UInt64 max_block_size
    , UInt64 max_streams)
{
    if (table_cypress_path.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cypress path are empty for ytsarurus source factory.");
    }
    String reason;
    const YTsaurusSettings& settings = source_options.settings;
    String cypress_path;
    if (settings[YTsaurusSetting::use_lock])
    {
        source_options.table_lock = std::make_shared<YTsaurusTableLock>(client, table_cypress_path, source_options.settings[YTsaurusSetting::transaction_timeout_ms].totalMilliseconds());
        cypress_path = source_options.table_lock->getNodePath();
    }
    else
    {
        cypress_path = table_cypress_path;
    }

    if (source_options.settings[YTsaurusSetting::check_table_schema] && !client->checkSchemaCompatibility(cypress_path, sample_block, reason, source_options.check_types_allow_nullable))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ClickHouse table schema doesn't match with yt table. Reason: {}", reason);
    }
    auto yt_node_type = client->getNodeType(cypress_path);
    if (yt_node_type == YTsaurusNodeType::STATIC_TABLE)
    {
        auto rows_count = client->getTableNumberOfRows(cypress_path);
        size_t max_streams_allowed;

        if (!max_streams)
            max_streams_allowed = settings[YTsaurusSetting::max_streams];
        else
            max_streams_allowed = std::min<size_t>(settings[YTsaurusSetting::max_streams], max_streams);

        size_t pipes_num = max_streams_allowed;

        auto min_rows_for_spawn_stream = settings[YTsaurusSetting::min_rows_for_spawn_stream];
        if (min_rows_for_spawn_stream)
            pipes_num = std::min(max_streams_allowed,
                std::max<size_t>(1u, rows_count / settings[YTsaurusSetting::min_rows_for_spawn_stream]
            ));

        size_t rows_batch_count = rows_count / pipes_num;
        Pipes pipes;
        LOG_DEBUG(::getLogger("YTsaurusSourceFactory"), "Will read static table {} with {} streams and rows {} in each", table_cypress_path, pipes_num, rows_batch_count);

        for (auto i = 0u; i < pipes_num; ++i)
        {
            size_t row_from = i * rows_batch_count;
            size_t row_to = (i + 1 == pipes_num) ? rows_count :  (i + 1) * rows_batch_count;
            YTsaurusClientPtr client_for_source(new YTsaurusClient(*client));
            pipes.emplace_back(std::make_shared<YTsaurusTableSourceStaticTable>(
                  client_for_source
                , cypress_path
                , std::make_pair(row_from, row_to)
                , source_options
                , sample_block
                , max_block_size
            ));
        }
        auto pipe = Pipe::unitePipes(std::move(pipes));
        pipe.resize(pipes_num);
        return pipe;
    }
    else if (yt_node_type == YTsaurusNodeType::DYNAMIC_TABLE)
    {
        return Pipe(std::make_shared<YTsaurusTableSourceDynamicTable>(client, cypress_path, source_options, sample_block, max_block_size));
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Node {} has unsupported type.", cypress_path);
    }
}

}
#endif
