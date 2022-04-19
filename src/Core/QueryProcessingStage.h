#pragma once

#include <base/types.h>


namespace DB
{

/// Up to what stage the SELECT query is executed or needs to be executed.
namespace QueryProcessingStage
{
    /// Numbers matter - the later stage has a larger number.
    ///
    /// It is part of Protocol ABI, add values only to the end.
    /// Also keep in mind that the code may depends on the order of fields, so be double aware when you will add new values.
    enum Enum
    {
        /// Only read/have been read the columns specified in the query.
        FetchColumns       = 0,
        /// Until the stage where the results of processing on different servers can be combined.
        WithMergeableState = 1,
        /// Completely.
        Complete           = 2,
        /// Until the stage where the aggregate functions were calculated and finalized.
        ///
        /// It is used for auto distributed_group_by_no_merge optimization for distributed engine.
        /// (See comments in StorageDistributed).
        WithMergeableStateAfterAggregation = 3,
        /// Same as WithMergeableStateAfterAggregation but also will apply limit on each shard.
        ///
        /// This query stage will be used for auto
        /// distributed_group_by_no_merge/distributed_push_down_limit
        /// optimization.
        /// (See comments in StorageDistributed).
        WithMergeableStateAfterAggregationAndLimit = 4,

        MAX = 5,
    };

    inline const char * toString(UInt64 stage)
    {
        static const char * data[] =
        {
            "FetchColumns",
            "WithMergeableState",
            "Complete",
            "WithMergeableStateAfterAggregation",
            "WithMergeableStateAfterAggregationAndLimit",
        };
        return stage < MAX
            ? data[stage]
            : "Unknown stage";
    }

    /// This method is used for the program options,
    /// hence it accept under_score notation for stage:
    /// - complete
    /// - fetch_columns
    /// - with_mergeable_state
    /// - with_mergeable_state_after_aggregation
    Enum fromString(const std::string & stage_string);
}

}
