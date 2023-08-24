//#pragma once
//
//#include <QueryCoordination/Fragments/StepMatcherBase.h>
//
//namespace DB
//{
//
//class OrderByMatcher : public StepMatcherBase
//{
//    PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs child_res, Data & data) override
//    {
//        auto child_fragment = child_res[0];
//        child_fragment->addStep(node.step);
//        if (!child_fragment->isPartitioned())
//        {
//            return child_fragment;
//        }
//
//        auto * sort_step = dynamic_cast<SortingStep *>(node.step.get());
//
//        /// Create a new fragment for a sort-merging exchange.
//        PlanFragmentPtr merge_fragment = createParentFragment(child_fragment, DataPartition{.type = PartitionType::UNPARTITIONED});
//        auto * exchange_node = merge_fragment->getRootNode(); /// exchange node
//
//        const SortDescription & sort_description = sort_step->getSortDescription();
//        const UInt64 limit = sort_step->getLimit();
//        const auto max_block_size = context->getSettingsRef().max_block_size;
//        const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;
//
//        ExchangeDataStep::SortInfo sort_info{
//            .max_block_size = max_block_size,
//            .always_read_till_end = exact_rows_before_limit,
//            .limit = limit,
//            .result_description = sort_description};
//
//        auto * exchange_step = dynamic_cast<ExchangeDataStep *>(exchange_node->step.get());
//        exchange_step->setSortInfo(sort_info);
//
//        all_fragments.emplace_back(merge_fragment);
//
//        return merge_fragment;
//    }
//};
//
//}
