#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Block.h>

namespace DB
{

class ActionsDAG;
struct Settings;
class KeyCondition;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;
struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

/// Some formats needs to custom mapping between columns in file and clickhouse columns.
class ColumnMapper
{
public:
    /// clickhouse_column_name -> field_id
    /// For tuples, the map contains both the tuple itself and all its elements, e.g. {t, t.x, t.y}.
    /// Note that parquet schema reader has to apply the mapping to all tuple fields recursively
    /// even if the whole tuple was requested, because the names of the fields may be different.
    void setStorageColumnEncoding(std::unordered_map<String, Int64> && storage_encoding_);

    const std::unordered_map<String, Int64> & getStorageColumnEncoding() const { return storage_encoding; }
    const std::unordered_map<Int64, String> & getFieldIdToClickHouseName() const { return field_id_to_clickhouse_name; }

    /// clickhouse_column_name -> format_column_name (just join the maps above by field_id).
    std::pair<std::unordered_map<String, String>, std::unordered_map<String, String>> makeMapping(const std::unordered_map<Int64, String> & format_encoding);

private:
    std::unordered_map<String, Int64> storage_encoding;
    std::unordered_map<Int64, String> field_id_to_clickhouse_name;
};

using ColumnMapperPtr = std::shared_ptr<ColumnMapper>;

struct FormatFilterInfo;
using FormatFilterInfoPtr = std::shared_ptr<FormatFilterInfo>;

/// Information telling which columns and rows to read when parsing a format.
/// Can be shared across multiple IInputFormat instances in one query to avoid doing the same
/// preprocessing multiple times. E.g. `SELECT ... FROM file('part{00..99}.parquet')`.
///
/// All nontrivial parts of this struct are lazily initialized by the IInputFormat implementation,
/// because most implementations don't use most of this struct.
struct FormatFilterInfo
{
    FormatFilterInfo(
        std::shared_ptr<const ActionsDAG> filter_actions_dag_,
        const ContextPtr & context_,
        ColumnMapperPtr column_mapper_,
        FilterDAGInfoPtr row_level_filter_,
        PrewhereInfoPtr prewhere_info_);

    FormatFilterInfo();

    std::shared_ptr<const ActionsDAG> filter_actions_dag;
    ContextWeakPtr context; // required only if `filter_actions_dag` is set
    FilterDAGInfoPtr row_level_filter;
    PrewhereInfoPtr prewhere_info; // assigned only if the format supports prewhere

    /// Optionally created from filter_actions_dag, if the format needs it.
    std::shared_ptr<const KeyCondition> key_condition;
    /// Columns that are only needed for PREWHERE. In key_condition's "key" tuple, they come after
    /// all columns of the sample block.
    Block additional_columns;

    /// IInputFormat implementation may put arbitrary state here.
    std::shared_ptr<void> opaque;

    ColumnMapperPtr column_mapper;

private:
    /// For lazily initializing the fields above.
    std::once_flag init_flag;
    std::exception_ptr init_exception;

public:
    bool hasFilter() const;

    /// Creates `key_condition` and `additional_columns`.
    /// Call inside initOnce.
    void initKeyCondition(const Block & keys);

    /// Does std::call_once(init_flag, ...).
    /// If a previous init attempt threw exception, rethrows it instead retrying.
    void initOnce(std::function<void()> f);
};

}
