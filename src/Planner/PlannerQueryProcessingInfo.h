#pragma once

#include <Common/Exception.h>
#include <Core/QueryProcessingStage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class PlannerQueryProcessingInfo
{
public:
    PlannerQueryProcessingInfo(QueryProcessingStage::Enum from_stage_, QueryProcessingStage::Enum to_stage_)
        : from_stage(from_stage_)
        , to_stage(to_stage_)
    {
        if (isIntermediateStage())
        {
            if (isFirstStage() || isSecondStage())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Query with intermediate stage cannot have any other stages");
        }

        if (isFromAggregationState())
        {
            if (isIntermediateStage() || isFirstStage() || isSecondStage())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Query with after aggregation stage cannot have any other stages");
        }
    }

    QueryProcessingStage::Enum getFromStage() const
    {
        return from_stage;
    }

    QueryProcessingStage::Enum getToStage() const
    {
        return to_stage;
    }

    /** Do I need to perform the first part of the pipeline?
      * Running on remote servers during distributed processing or if query is not distributed.
      *
      * Also note that with distributed_group_by_no_merge=1 or when there is
      * only one remote server, it is equal to local query in terms of query
      * stages (or when due to optimize_distributed_group_by_sharding_key the query was processed up to Complete stage).
      */
    bool isFirstStage() const
    {
        return from_stage < QueryProcessingStage::WithMergeableState
            && to_stage >= QueryProcessingStage::WithMergeableState;
    }

    /** Do I need to execute the second part of the pipeline?
      * Running on the initiating server during distributed processing or if query is not distributed.
      *
      * Also note that with distributed_group_by_no_merge=2 (i.e. when optimize_distributed_group_by_sharding_key takes place)
      * the query on the remote server will be processed up to WithMergeableStateAfterAggregationAndLimit,
      * So it will do partial second stage (second_stage=true), and initiator will do the final part.
      */
    bool isSecondStage() const
    {
        return from_stage <= QueryProcessingStage::WithMergeableState
            && to_stage > QueryProcessingStage::WithMergeableState;
    }

    bool isIntermediateStage() const
    {
        return from_stage == QueryProcessingStage::WithMergeableState && to_stage == QueryProcessingStage::WithMergeableState;
    }

    bool isToAggregationState() const
    {
        return to_stage >= QueryProcessingStage::WithMergeableStateAfterAggregation;
    }

    bool isFromAggregationState() const
    {
        return from_stage >= QueryProcessingStage::WithMergeableStateAfterAggregation;
    }
private:
    QueryProcessingStage::Enum from_stage;
    QueryProcessingStage::Enum to_stage;
};

}
