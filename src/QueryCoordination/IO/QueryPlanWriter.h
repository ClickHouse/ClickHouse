#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ScanStep.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>


/* all extends IQueryPlanStep
AggregatingProjectionStep (DB)
CreatingSetsStep (DB)
IntersectOrExceptStep (DB)
ISourceStep (DB)
    ReadFromParallelRemoteReplicasStep (DB)
    ReadFromPart (DB)
    ReadFromPreparedSource (DB)
        ReadFromStorageStep (DB)
    ReadFromRemote (DB)
    ReadNothingStep (DB)
    SourceStepWithFilter (DB)
        ReadFromDummy (DB)
        ReadFromMemoryStorageStep (DB)
        ReadFromMerge (DB)
        ReadFromMergeTree (DB)
        ReadFromSystemZooKeeper (DB)
ITransformingStep (DB)
    AggregatingStep (DB)
    ArrayJoinStep (DB)
    CreateSetAndFilterOnTheFlyStep (DB)
    CreatingSetStep (DB)
    CubeStep (DB)
    DistinctStep (DB)
    ExpressionStep (DB)
    ExtremesStep (DB)
    FilledJoinStep (DB)
    FillingStep (DB)
    FilterStep (DB)
    LimitByStep (DB)
    LimitStep (DB)
    MergingAggregatedStep (DB)
    OffsetStep (DB)
    RollupStep (DB)
    SortingStep (DB)
    TotalsHavingStep (DB)
    WindowStep (DB)
JoinStep (DB)
UnionStep (DB)
 *
 * */

namespace DB
{

class QueryPlanWriter
{

    void write(const Block & header, WriteBuffer & out) const
    {

    }

    void write(const SortDescription & sort_description, WriteBuffer & out) const
    {
        Coordination::write(sort_description.compile_sort_description, out);
        Coordination::write(sort_description.min_count_to_compile_sort_description, out);
    }

    void write(const DataStream::SortScope & sort_scope, WriteBuffer & out) const
    {

        //            enum class SortScope
        //            {
        //                None   = 0,
        //                Chunk  = 1, /// Separate chunks are sorted
        //                Stream = 2, /// Each data steam is sorted
        //                Global = 3, /// Data is globally sorted
        //            };
        switch (sort_scope)
        {
            case DataStream::SortScope::None:
                Coordination::write(0, out);
            case DataStream::SortScope::Chunk:
                Coordination::write(1, out);
            case DataStream::SortScope::Stream:
                Coordination::write(2, out);
            case DataStream::SortScope::Global:
                Coordination::write(3, out);
        }
    }

    void write(const DataStreams & streams, WriteBuffer & out) const
    {
        for (const auto & stream : streams)
        {
            write(stream.header, out);
            Coordination::write(stream.has_single_port, out);
            write(stream.sort_description, out);
            write(stream.sort_scope, out);
        }
    }

    void write(const Aggregator::Params & params, WriteBuffer & out) const
    {
        for (const auto & param : params)
        {

        }
    }

    friend class AggregatingStep;
    void write(std::shared_ptr<AggregatingStep> aggregating_step, WriteBuffer & out) const
    {
//        AggregatingStep(
//            const DataStream & input_stream_,
//            Aggregator::Params params_,
//            GroupingSetsParamsList grouping_sets_params_,
//            bool final_,
//            size_t max_block_size_,
//            size_t aggregation_in_order_max_block_bytes_,
//            size_t merge_threads_,
//            size_t temporary_data_merge_threads_,
//            bool storage_has_evenly_distributed_read_,
//            bool group_by_use_nulls_,
//            SortDescription sort_description_for_merging_,
//            SortDescription group_by_sort_description_,
//            bool should_produce_results_in_order_of_bucket_number_,
//            bool memory_bound_merging_of_aggregation_results_enabled_,
//            bool explicit_sorting_required_for_aggregation_in_order_);

        write(aggregating_step->getInputStreams(), out);

        aggregating_step->getParams();

    }

    void write(std::shared_ptr<AggregatingStep> aggregating_step, WriteBuffer & out) const
    {
        Coordination::write(query_id, out);

        fragment->write(out);

        Coordination::write(destinations.size(), out);
        for (const DestinationRequest & destination : destinations)
        {
            destination.write(out);
        }
    }

    void write(std::shared_ptr<AggregatingStep> aggregating_step, WriteBuffer & out) const
    {
        Coordination::write(query_id, out);

        fragment->write(out);

        Coordination::write(destinations.size(), out);
        for (const DestinationRequest & destination : destinations)
        {
            destination.write(out);
        }
    }

    void write(std::shared_ptr<AggregatingStep> aggregating_step, WriteBuffer & out) const
    {
        Coordination::write(query_id, out);

        fragment->write(out);

        Coordination::write(destinations.size(), out);
        for (const DestinationRequest & destination : destinations)
        {
            destination.write(out);
        }
    }
};

}
