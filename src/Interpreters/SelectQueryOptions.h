#pragma once

#include <Core/QueryProcessingStage.h>
#include <optional>

namespace DB
{

/**
 * to_stage
 * - the stage to which the query is to be executed. By default - till to the end.
 *   You can perform till the intermediate aggregation state, which are combined from different servers for distributed query processing.
 *
 * subquery_depth
 * - to control the limit on the depth of nesting of subqueries. For subqueries, a value that is incremented by one is passed;
 *   for INSERT SELECT, a value 1 is passed instead of 0.
 *
 * only_analyze
 * - the object was created only for query analysis.
 *
 * is_subquery
 * - there could be some specific for subqueries. Ex. there's no need to pass duplicated columns in results, cause of indirect results.
 *
 * is_internal
 * - the object was created only for internal queries.
 */
struct SelectQueryOptions
{
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    bool only_analyze = false;
    bool modify_inplace = false;
    bool remove_duplicates = false;
    bool ignore_quota = false;
    bool ignore_limits = false;
    /// This flag is needed for projection description.
    /// Otherwise, keys for GROUP BY may be removed as constants.
    bool ignore_ast_optimizations = false;
    bool ignore_alias = false;
    bool is_internal = false;
    bool is_subquery = false; // non-subquery can also have subquery_depth > 0, e.g. insert select
    bool with_all_cols = false; /// asterisk include materialized and aliased columns
    bool settings_limit_offset_done = false;
    bool is_explain = false; /// The value is true if it's explain statement.
    bool is_create_parameterized_view = false;
    /// Bypass setting constraints for some internal queries such as projection ASTs.
    bool ignore_setting_constraints = false;

    /// Bypass access check for select query.
    /// This allows to skip double access check in some specific cases (e.g. insert into table with materialized view)
    bool ignore_access_check = false;

    /// These two fields are used to evaluate shardNum() and shardCount() function when
    /// prefer_localhost_replica == 1 and local instance is selected. They are needed because local
    /// instance might have multiple shards and scalars can only hold one value.
    std::optional<UInt32> shard_num;
    std::optional<UInt32> shard_count;

    /** During read from MergeTree parts will be removed from snapshot after they are not needed.
      * This optimization will break subsequent execution of the same query tree, because table node
      * will no more have valid snapshot.
      *
      * TODO: Implement this functionality in safer way
      */
    bool merge_tree_enable_remove_parts_from_snapshot_optimization = true;

    SelectQueryOptions( /// NOLINT(google-explicit-constructor)
        QueryProcessingStage::Enum stage = QueryProcessingStage::Complete,
        size_t depth = 0,
        bool is_subquery_ = false,
        bool settings_limit_offset_done_ = false)
        : to_stage(stage), subquery_depth(depth), is_subquery(is_subquery_),
        settings_limit_offset_done(settings_limit_offset_done_)
    {}

    SelectQueryOptions copy() const { return *this; }

    SelectQueryOptions subquery() const
    {
        SelectQueryOptions out = *this;
        out.to_stage = QueryProcessingStage::Complete;
        ++out.subquery_depth;
        out.is_subquery = true;
        return out;
    }

    SelectQueryOptions createParameterizedView() const
    {
        SelectQueryOptions out = *this;
        out.is_create_parameterized_view = true;
        return out;
    }

    SelectQueryOptions & analyze(bool dry_run = true)
    {
        only_analyze = dry_run;
        return *this;
    }

    SelectQueryOptions & modify(bool value = true)
    {
        modify_inplace = value;
        return *this;
    }

    SelectQueryOptions & noModify() { return modify(false); }

    SelectQueryOptions & removeDuplicates(bool value = true)
    {
        remove_duplicates = value;
        return *this;
    }

    SelectQueryOptions & noSubquery()
    {
        subquery_depth = 0;
        return *this;
    }

    SelectQueryOptions & ignoreLimits(bool value = true)
    {
        ignore_limits = value;
        return *this;
    }

    SelectQueryOptions & ignoreAlias(bool value = true)
    {
        ignore_alias = value;
        return *this;
    }

    SelectQueryOptions & ignoreASTOptimizations(bool value = true)
    {
        ignore_ast_optimizations = value;
        return *this;
    }

    SelectQueryOptions & ignoreSettingConstraints(bool value = true)
    {
        ignore_setting_constraints = value;
        return *this;
    }

    SelectQueryOptions & ignoreAccessCheck(bool value = true)
    {
        ignore_access_check = value;
        return *this;
    }

    SelectQueryOptions & setInternal(bool value = false)
    {
        is_internal = value;
        return *this;
    }

    SelectQueryOptions & setWithAllColumns(bool value = true)
    {
        with_all_cols = value;
        return *this;
    }

    SelectQueryOptions & setShardInfo(UInt32 shard_num_, UInt32 shard_count_)
    {
        shard_num = shard_num_;
        shard_count = shard_count_;
        return *this;
    }

    SelectQueryOptions & setExplain(bool value = true)
    {
        is_explain = value;
        return *this;
    }
};

}
