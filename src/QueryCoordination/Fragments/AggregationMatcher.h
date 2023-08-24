//#pragma once
//
//#include <QueryCoordination/Fragments/StepMatcherBase.h>
//
//namespace DB
//{
//
//class AggregationMatcher : public StepMatcherBase
//{
//    PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs child_res, Data & data) override
//    {
//        auto child_fragment = child_res[0];
//        auto * aggregate_step = dynamic_cast<AggregatingStep *>(step.get());
//
//        auto aggregating_step = aggregate_step->clone(false);
//
//        child_fragment->addStep(aggregating_step);
//
//        DataPartition partition;
//        //    if (expressions.group_by_elements_actions.empty())
//        if (!aggregating_step->getParams().keys_size || aggregating_step->withTotalsOrCubeOrRollup())
//        {
//            partition.type = PartitionType::UNPARTITIONED;
//        }
//        else
//        {
//            partition.type = PartitionType::HASH_PARTITIONED;
//            partition.keys = aggregating_step->getParams().keys;
//            partition.keys_size = aggregating_step->getParams().keys_size;
//            partition.partition_by_bucket_num = true;
//        }
//
//        // place a merge aggregation step in a new fragment
//        PlanFragmentPtr merge_fragment = createParentFragment(child_fragment, partition);
//
//        const Settings & settings = context->getSettingsRef();
//        std::shared_ptr<MergingAggregatedStep> merge_agg_node = aggregating_step->makeMergingAggregatedStep(merge_fragment->getCurrentDataStream(), settings);
//
//        merge_fragment->addStep(std::move(merge_agg_node));
//
//        all_fragments.emplace_back(merge_fragment);
//
//        return merge_fragment;
//    }
//};
//
//}
