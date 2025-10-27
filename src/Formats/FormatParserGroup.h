#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class ActionsDAG;
struct Settings;
class KeyCondition;
struct FormatParserGroup;

using FormatParserGroupPtr = std::shared_ptr<FormatParserGroup>;

/// Some formats needs to custom mapping between columns in file and clickhouse columns.
class ColumnMapper
{
public:
    /// clickhouse_column_name -> field_id
    void setStorageColumnEncoding(std::unordered_map<String, Int64> && storage_encoding_);

    /// clickhouse_column_name -> format_column_name (just join the maps above by field_id).
    std::pair<std::unordered_map<String, String>, std::unordered_map<String, String>> makeMapping(
        const Block & header,
        const std::unordered_map<Int64, String> & format_encoding);

private:
    std::unordered_map<String, Int64> storage_encoding;
};

using ColumnMapperPtr = std::shared_ptr<ColumnMapper>;

/// When reading many files in one query, e.g. `SELECT ... FROM file('part{00..99}.parquet')`,
/// we want the file readers to share some resource limits, e.g. number of threads.
/// They may also want to share some data structures to avoid initializing multiple copies,
/// e.g. KeyCondition.
/// This struct is shared among such group of readers (IInputFormat instances).
/// All nontrivial parts of this struct are lazily initialized by the IInputFormat implementation,
/// because most implementations don't use most of this struct.
struct FormatParserGroup
{
    /// Total limits across all readers in the group.
    size_t max_parsing_threads = 0;
    size_t max_io_threads = 0;

    std::atomic<size_t> num_streams {0};

    std::shared_ptr<const ActionsDAG> filter_actions_dag;
    ContextWeakPtr context; // required only if `filter_actions_dag` is set
    /// TODO: std::optional<const ExpressionActions> prewhere_actions;

    ThreadPoolCallbackRunnerFast parsing_runner;
    ThreadPoolCallbackRunnerFast io_runner;

    /// Optionally created from filter_actions_dag, if the format needs it.
    std::shared_ptr<const KeyCondition> key_condition;

    /// IInputFormat implementation may put arbitrary state here.
    std::shared_ptr<void> opaque;

    ColumnMapperPtr column_mapper;

private:
    /// For lazily initializing the fields above.
    std::once_flag init_flag;
    std::exception_ptr init_exception;

public:
    FormatParserGroup(const Settings & settings, size_t num_streams_, std::shared_ptr<const ActionsDAG> filter_actions_dag_, const ContextPtr & context_);

    static FormatParserGroupPtr singleThreaded(const Settings & settings);

    bool hasFilter() const;

    void finishStream();

    size_t getParsingThreadsPerReader() const;
    size_t getIOThreadsPerReader() const;

    /// Creates `key_condition`. Call inside call_once(init_flag, ...).
    void initKeyCondition(const Block & keys);

    /// Does std::call_once(init_flag, ...).
    /// If a previous init attempt threw exception, rethrows it instead retrying.
    void initOnce(std::function<void()> f);
};

}
