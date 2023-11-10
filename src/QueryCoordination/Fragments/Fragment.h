#pragma once

#include <memory>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Exchange/ExchangeDataSink.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

class Fragment : public std::enable_shared_from_this<Fragment>
{
public:
    using Node = QueryPlan::Node;
    using Nodes = std::list<Node>;

    /// Explain options for distributed plan, only work with query coordination
    struct ExplainFragmentOptions
    {
        /// Add output header to step.
        bool header = false;
        /// Add description of step.
        bool description = true;
        /// Add detailed information about step actions.
        bool actions = false;
        /// Add information about indexes actions.
        bool indexes = false;
        /// Add information about sorting
        bool sorting = false;
        /// Add fragment and host mappings information
        bool host = false; /// TODO implement
    };

    Fragment(UInt32 fragment_id_, ContextMutablePtr context_);

    void addStep(QueryPlanStepPtr step);
    void uniteFragments(QueryPlanStepPtr step, FragmentPtrs & fragments);

    void setDestination(Node * dest_exchange, FragmentPtr dest_fragment);

    const DataStream & getCurrentDataStream() const;

    Node * getRoot() const;
    const Nodes & getNodes() const;

    void dump(WriteBufferFromOwnString & buffer, const ExplainFragmentOptions & settings);
    const FragmentPtrs & getChildren() const;

    UInt32 getFragmentID() const;
    UInt32 getDestFragmentID() const;
    bool hasDestFragment() const;

    UInt32 getDestExchangeID() const;
    const Node * getDestExchangeNode() const;

    QueryPipeline buildQueryPipeline(std::vector<ExchangeDataSink::Channel> & channels, const String & local_host);

    void explainPipeline(WriteBuffer & buffer, bool show_header);

private:
    bool isInitialized() const;

    Node makeNewNode(QueryPlanStepPtr step, std::vector<PlanNode *> children_ = {});
    void explainPlan(WriteBuffer & buffer, const ExplainFragmentOptions & settings);

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings);

private:
    UInt32 fragment_id;

    UInt32 plan_id_counter;

    Nodes nodes;
    Node * root;

    Node * dest_exchange_node;
    UInt32 dest_fragment_id;
    FragmentPtrs children;

    ContextMutablePtr context;
};

}
