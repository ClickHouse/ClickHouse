#include <Planner/CollectLegacyActionNodeNames.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

class CollectLegacyActionNodeNamesVisitor : public ConstInDepthQueryTreeVisitor<CollectLegacyActionNodeNamesVisitor>
{
public:
    explicit CollectLegacyActionNodeNamesVisitor(const PlannerContextPtr & planner_context_)
        : planner_context(planner_context_)
        , legacy_names(planner_context_->getGlobalPlannerContext()->getLegacyActionNodeNames())
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        /// Only the node types calculateActionNodeName renders; it throws on any other.
        auto node_type = node->getNodeType();
        if (node_type != QueryTreeNodeType::COLUMN && node_type != QueryTreeNodeType::FUNCTION)
            return;

        /// The name cache is keyed by node alone, so each convention needs its own.
        auto current_name = calculateActionNodeName(
            node, *planner_context, current_names_cache, true, ColumnIdentifierConvention::Current);

        auto legacy_name = calculateActionNodeName(
            node, *planner_context, legacy_names_cache, true, ColumnIdentifierConvention::Legacy,
            &unrenderable_legacy_nodes);

        if (unrenderable_legacy_nodes.contains(node))
            legacy_names.addAmbiguous(current_name);
        else
            legacy_names.add(current_name, legacy_name);
    }

    static bool needChildVisit(const QueryTreeNodePtr & /*parent*/, const QueryTreeNodePtr & child)
    {
        /// Nested queries are collected by their own planner. Table function arguments may hold unresolved
        /// identifiers (named-collection `key = value` overrides) that calculateActionNodeName cannot name.
        auto child_type = child->getNodeType();
        return child_type != QueryTreeNodeType::QUERY && child_type != QueryTreeNodeType::UNION
            && child_type != QueryTreeNodeType::TABLE_FUNCTION && child_type != QueryTreeNodeType::TABLE;
    }

private:
    const PlannerContextPtr & planner_context;
    LegacyActionNodeNames & legacy_names;
    QueryTreeNodeToName current_names_cache;
    QueryTreeNodeToName legacy_names_cache;
    QueryTreeNodeSet unrenderable_legacy_nodes;
};

}

void collectLegacyActionNodeNames(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context)
{
    CollectLegacyActionNodeNamesVisitor visitor(planner_context);
    visitor.visit(query_tree);
}

LegacyHeaderNames buildLegacyHeaderNames(
    const Block & header, const PlannerContextPtr & planner_context, QueryProcessingStage::Enum stage)
{
    if (!planner_context || stage == QueryProcessingStage::Complete || stage == QueryProcessingStage::QueryPlan)
        return {};

    const auto & legacy_names = planner_context->getGlobalPlannerContext()->getLegacyActionNodeNames();
    if (legacy_names.hasNoCandidates())
        return {};

    LegacyHeaderNames result;
    for (const auto & column : header)
    {
        auto lookup = legacy_names.getLegacyName(column.name);
        if (lookup.state == LegacyActionNodeNames::State::Unique)
            result.current_to_legacy.emplace(column.name, std::move(lookup.legacy_name));
        else if (lookup.state == LegacyActionNodeNames::State::Ambiguous)
            result.ambiguous.emplace(column.name);
    }
    return result;
}

void renameOutgoingHeaderForLegacyUpstream(
    QueryPlan & query_plan,
    const PlannerContextPtr & planner_context,
    const SelectQueryOptions & select_query_options,
    const ContextPtr & context)
{
    const auto & client_info = context->getClientInfo();
    if (client_info.query_kind != ClientInfo::QueryKind::SECONDARY_QUERY
        || select_query_options.is_local_plan_for_distributed_query)
        return;

    /// The immediate upstream's version; TCP only, 0 when the peer reports none.
    VersionNumber upstream_version(
        client_info.connection_client_version_major,
        client_info.connection_client_version_minor,
        client_info.connection_client_version_patch);
    if (upstream_version == VersionNumber{} || upstream_version >= legacy_quoted_identifiers_version)
        return;

    const auto & header = *query_plan.getCurrentHeader();
    auto legacy_names = buildLegacyHeaderNames(header, planner_context, select_query_options.to_stage);
    if (legacy_names.empty())
        return;

    auto renamed = header.getColumnsWithTypeAndName();
    NameToNameMap outgoing_to_current_name;
    for (auto & column : renamed)
    {
        auto it = legacy_names.current_to_legacy.find(column.name);
        String outgoing_name = it != legacy_names.current_to_legacy.end() ? it->second : column.name;

        auto [claimed, inserted] = outgoing_to_current_name.emplace(outgoing_name, column.name);
        if (legacy_names.ambiguous.contains(column.name) || (!inserted && claimed->second != column.name))
            throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                "Cannot send column {} to a server older than {}: its plan column identifier has no unambiguous pre-{} "
                "spelling among the columns of {}", column.name, legacy_quoted_identifiers_version.toString(),
                legacy_quoted_identifiers_version.toString(), header.dumpNames());

        column.name = std::move(outgoing_name);
    }

    auto rename_dag = ActionsDAG::makeConvertingActions(
        header.getColumnsWithTypeAndName(), renamed, ActionsDAG::MatchColumnsMode::Position, context);
    auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(rename_dag));
    rename_step->setStepDescription("Rename plan column identifiers for a pre-26.5 upstream");
    query_plan.addStep(std::move(rename_step));
}

}
