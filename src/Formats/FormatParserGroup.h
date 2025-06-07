#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Block.h>

namespace DB
{

class ActionsDAG;
struct Settings;
class KeyCondition;
struct FormatParserGroup;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

using FormatParserGroupPtr = std::shared_ptr<FormatParserGroup>;

/// When reading many files in one query, e.g. `SELECT ... FROM file('part{00..99}.parquet')`,
/// we want the file readers to share some resource limits, e.g. number of threads.
/// They may also want to share some data structures to avoid initializing multiple copies,
/// e.g. KeyCondition.
/// This struct is shared among such group of readers (IInputFormat instances).
///
/// All nontrivial parts of this struct are lazily initialized by the IInputFormat implementation,
/// because most implementations don't use most of this struct.
///
/// The lazy initialization assumes that all readers of the group have the same format and
/// FormatSettings, same sample block, same WHERE/PREWHERE conditions, same Context.
struct FormatParserGroup
{
    /// Total limits across all readers in the group.
    size_t max_parsing_threads = 0;
    size_t max_io_threads = 0;

    std::atomic<size_t> num_streams {0};

    std::shared_ptr<const ActionsDAG> filter_actions_dag;
    ContextWeakPtr context; // required only if `filter_actions_dag` is set
    PrewhereInfoPtr prewhere_info; // assigned only if the format supports prewhere

    ThreadPoolCallbackRunnerFast parsing_runner;
    ThreadPoolCallbackRunnerFast io_runner;

    /// Optionally created from filter_actions_dag, if the format needs it.
    std::shared_ptr<const KeyCondition> key_condition;
    std::unordered_set<size_t> columns_used_by_key_condition;
    /// Columns that are only needed for PREWHERE. In key_condition's "key" tuple, they come after
    /// all columns of the sample block.
    Block additional_columns;

    /// IInputFormat implementation may put arbitrary state here.
    std::shared_ptr<void> opaque;

    /// For lazily initializing the fields above.
    std::once_flag init_flag;


    FormatParserGroup(const Settings & settings, size_t num_streams_, std::shared_ptr<const ActionsDAG> filter_actions_dag_, const ContextPtr & context_);

    static FormatParserGroupPtr singleThreaded(const Settings & settings);

    bool hasFilter() const;

    void finishStream();

    size_t getParsingThreadsPerReader() const;
    size_t getIOThreadsPerReader() const;

    /// Creates `key_condition`, `additional_columns`, and `columns_used_by_key_condition`.
    /// Call inside call_once(init_flag, ...).
    void initKeyCondition(const Block & keys);
};

}
