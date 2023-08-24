//#pragma once
//
//#include <QueryCoordination/Fragments/StepMatcherBase.h>
//
//namespace DB
//{
//
//class JoinMatcher : public StepMatcherBase
//{
//    PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs /*child_res*/, Data & data) override
//    {
//        auto * join_step = dynamic_cast<JoinStep *>(step.get());
//
//        JoinPtr join = join_step->getJoin();
//        const TableJoin & table_join = join->getTableJoin();
//
//        if (table_join.getClauses().size() != 1 || table_join.strictness() == JoinStrictness::Asof) /// broadcast join. Asof support != condition
//        {
//            /// join_step push down left fragment, right fragment as left fragment child
//            left_child_fragment->addChildPlanFragments(step, {right_child_fragment});
//            right_child_fragment->setOutputPartition(DataPartition{.type = PartitionType::UNPARTITIONED});
//            return left_child_fragment;
//        }
//        else
//        {
//            auto join_clause = table_join.getOnlyClause(); /// Definitely equals condition
//            DataPartition lhs_join_partition{.type = HASH_PARTITIONED, .keys = join_clause.key_names_left, .keys_size = join_clause.key_names_left.size(), .partition_by_bucket_num = false};
//
//            DataPartition rhs_join_partition{.type = HASH_PARTITIONED, .keys = join_clause.key_names_right, .keys_size = join_clause.key_names_right.size(), .partition_by_bucket_num = false};
//
//            PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(context->getFragmentID(), lhs_join_partition, context);
//
//            PlanFragmentPtrs left_right_fragments;
//            left_right_fragments.emplace_back(left_child_fragment);
//            left_right_fragments.emplace_back(right_child_fragment);
//
//            parent_fragment->unitePlanFragments(step, left_right_fragments, storage_limits);
//
//            left_child_fragment->setOutputPartition(lhs_join_partition);
//            right_child_fragment->setOutputPartition(rhs_join_partition);
//
//            all_fragments.emplace_back(parent_fragment);
//
//            return parent_fragment;
//        }
//    }
//};
//
//}
