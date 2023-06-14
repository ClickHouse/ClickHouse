#pragma once

#include <memory>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Processors/QueryPlan/ExchangeDataStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/DataSink.h>
#include <QueryCoordination/DataPartition.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

using FragmentID = Int32;
using PlanID = Int32;

/**
 * 1. PlanFragment 划分
 * 2. PlanFragment 节点调度
 * 3. PlanFragment 执行，fragment驱动执行（谁先谁后，谁调动谁执行），fragment 之间数据交互（sink，exchange）
 * 4. 数据如何写回到 tcphandler
 */
class PlanFragment : public std::enable_shared_from_this<PlanFragment>
{
public:
    using Node = QueryPlan::Node;

    explicit PlanFragment(ContextMutablePtr & context_, QueryPlanStepPtr step, DataPartition & partition)
        : context(context_), data_partition(partition)
    {
        query_plan.addStep(step);
        query_plan.getRootNode()->plan_id = ++plan_id_counter;
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    void setFragmentInPlanTree(Node * node)
    {
        if (!node || !node->step)
        {
            return;
        }
        node->fragment = shared_from_this();

        if (dynamic_cast<ExchangeDataStep *>(node->step.get()))
        {
            return;
        }

        for (Node * child : node->children)
        {
            setFragmentInPlanTree(child);
        }
    }

    // add plan root
    void addStep(QueryPlanStepPtr step)
    {
        query_plan.addStep(step);
        auto * root_node = query_plan.getRootNode();
        root_node->fragment = shared_from_this();
        root_node->plan_id = ++plan_id_counter;
    }

    const DataStream & getCurrentDataStream() const
    {
        return query_plan.getCurrentDataStream();
    }

    PlanFragmentPtr getDestFragment() const
    {
        if (!dest_node || !dest_node->step)
        {
            return nullptr;
        }
        return dest_node->fragment;
    }

    void addChild(PlanFragmentPtr fragment)
    {
        children.emplace_back(fragment);
    }

    void setDestination(Node * node)
    {
        dest_node = node;
        PlanFragmentPtr dest = getDestFragment();
        dest->addChild(shared_from_this());
    }

    Node * getRootNode() const { return query_plan.getRootNode(); }

    const DataPartition & getDataPartition() const { return data_partition; }

    bool isPartitioned() const { return data_partition.type != PartitionType::UNPARTITIONED; }

    void dump(WriteBufferFromOwnString & buffer)
    {
        QueryPlan::ExplainPlanOptions settings;
        buffer.write('\n');
        std::string str("Fragment " + std::to_string(fragment_id));
        buffer.write(str.c_str(), str.size());
        buffer.write('\n');
        query_plan.explainPlan(buffer, settings);

        for (const auto & child_fragment : children)
        {
            child_fragment->dump(buffer);
        }
    }

    QueryPlan & getQueryPlan() { return query_plan; }

    std::shared_ptr<Cluster> getCluster() const { return cluster; }

    void setCluster(std::shared_ptr<Cluster> cluster_) { cluster = cluster_; }

    void setFragmentId(UInt32 id) { fragment_id = id; }

    Int32 getFragmentId() const { return fragment_id; }

    QueryPipeline buildQueryPipeline(std::vector<DataSink::Channel> & channels);

private:

    ContextMutablePtr context;

    // id for this plan fragment
    FragmentID fragment_id;
    // nereids planner and original planner generate fragments in different order.
    // This makes nereids fragment id different from that of original planner, and
    // hence different from that in profile.
    // in original planner, fragmentSequenceNum is fragmentId, and in nereids planner,
    // fragmentSequenceNum is the id displayed in profile

//    Int32 fragmentSequenceNum;
    // private PlanId planId_;
    // private CohortId cohortId_;

    // query plan for Fragment
    QueryPlan query_plan;

    // exchange node to which this fragment sends its output
    Node * dest_node = nullptr;

    PlanFragmentPtrs children;

    DataPartition data_partition;

    DataPartition output_partition;

    // if null, outputs the entire row produced by planRoot
    // ArrayList<Expr> outputExprs;

    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment,
    // otherwise it is scheduled to the cluster node according to the DataPartition, the principle of minimum data movement.
    std::shared_ptr<Cluster> cluster;

    PlanID plan_id_counter = 0;
};

using PlanFragmentPtr = std::shared_ptr<PlanFragment>;
using PlanFragmentPtrs = std::vector<PlanFragmentPtr>;

}
