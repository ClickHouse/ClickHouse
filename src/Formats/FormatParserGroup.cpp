#include <Formats/FormatParserGroup.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsMaxThreads max_download_threads;
    extern const SettingsMaxThreads max_parsing_threads;
}

void ColumnMapper::setStorageColumnEncoding(std::unordered_map<String, Int64> && storage_encoding_)
{
    storage_encoding = std::move(storage_encoding_);
}

std::pair<std::unordered_map<String, String>, std::unordered_map<String, String>> ColumnMapper::makeMapping(
    const Block & header,
    const std::unordered_map<Int64, String> & format_encoding)
{
    std::unordered_map<String, String> clickhouse_to_parquet_names;
    std::unordered_map<String, String> parquet_names_to_clickhouse;

    for (size_t i = 0; i < header.columns(); ++i)
    {
        auto column_name = header.getNames()[i];
        int64_t field_id;
        if (auto it = storage_encoding.find(column_name); it != storage_encoding.end())
            field_id = it->second;
        else
            continue;
        if (auto it = format_encoding.find(field_id); it != format_encoding.end())
        {
            clickhouse_to_parquet_names[column_name] = it->second;
            parquet_names_to_clickhouse[it->second] = column_name;
        }
        else
        {
            clickhouse_to_parquet_names[column_name] = column_name;
            parquet_names_to_clickhouse[column_name] = column_name;
        }
    }
    return {clickhouse_to_parquet_names, parquet_names_to_clickhouse};
}

FormatParserGroup::FormatParserGroup(const Settings & settings, size_t num_streams_, std::shared_ptr<const ActionsDAG> filter_actions_dag_, const ContextPtr & context_)
    : max_parsing_threads(settings[Setting::max_parsing_threads])
    , max_io_threads(settings[Setting::max_download_threads])
    , num_streams(num_streams_)
    , filter_actions_dag(std::move(filter_actions_dag_))
    , context(ContextWeakPtr(context_)) {}

FormatParserGroupPtr FormatParserGroup::singleThreaded(const Settings & settings)
{
    auto parser_group = std::make_shared<FormatParserGroup>(settings, 1, nullptr, nullptr);
    parser_group->max_parsing_threads = 1;
    return parser_group;
}

bool FormatParserGroup::hasFilter() const
{
    return filter_actions_dag != nullptr;
}

void FormatParserGroup::finishStream()
{
    num_streams.fetch_sub(1, std::memory_order_relaxed);
}

size_t FormatParserGroup::getParsingThreadsPerReader() const
{
    size_t n = num_streams.load(std::memory_order_relaxed);
    n = std::max(n, 1ul);
    return (max_parsing_threads + n - 1) / n;
}

size_t FormatParserGroup::getIOThreadsPerReader() const
{
    size_t n = num_streams.load(std::memory_order_relaxed);
    n = std::max(n, 1ul);
    return (max_io_threads + n - 1) / n;
}

void FormatParserGroup::initKeyCondition(const Block & keys)
{
    if (!filter_actions_dag)
        return;

    auto ctx = context.lock();
    if (!ctx) throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");

    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), ctx);
    key_condition = std::make_shared<const KeyCondition>(
        inverted_dag, ctx, keys.getNames(),
        std::make_shared<ExpressionActions>(ActionsDAG(keys.getColumnsWithTypeAndName())));
}

void FormatParserGroup::initOnce(std::function<void()> f)
{
    std::call_once(init_flag, [&]
        {
            if (init_exception)
                std::rethrow_exception(init_exception);

            try
            {
                f();
            }
            catch (...)
            {
                init_exception = std::current_exception();
                throw;
            }
        });
}

}
