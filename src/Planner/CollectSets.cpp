#include <Planner/CollectSets.h>

#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>

#include <Storages/StorageSet.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Planner/Planner.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

class CollectSetsVisitor : public ConstInDepthQueryTreeVisitor<CollectSetsVisitor>
{
public:
    explicit CollectSetsVisitor(PlannerContext & planner_context_) //, const SelectQueryOptions & select_query_options_)
        : planner_context(planner_context_)
        //, select_query_options(select_query_options_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isNameOfInFunction(function_node->getFunctionName()))
            return;

        auto in_first_argument = function_node->getArguments().getNodes().at(0);
        auto in_second_argument = function_node->getArguments().getNodes().at(1);
        auto in_second_argument_node_type = in_second_argument->getNodeType();

        const auto & settings = planner_context.getQueryContext()->getSettingsRef();

        // String set_key = planner_context.createSetKey(in_second_argument);

        // if (planner_context.hasSet(set_key))
        //     return;

        auto & sets = planner_context.getPreparedSets();

        /// Tables and table functions are replaced with subquery at Analysis stage, except special Set table.
        auto * second_argument_table = in_second_argument->as<TableNode>();
        StorageSet * storage_set = second_argument_table != nullptr ? dynamic_cast<StorageSet *>(second_argument_table->getStorage().get()) : nullptr;

        if (storage_set)
        {
            /// Handle storage_set as ready set.
            auto set_key = PreparedSetKey::forSubquery(in_second_argument->getTreeHash());
            sets.addFromStorage(set_key, storage_set->getSet());
            //planner_context.registerSet(set_key, PlannerSet(FutureSet(storage_set->getSet())));
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = makeSetForConstantValue(
                in_first_argument->getResultType(),
                constant_node->getValue(),
                constant_node->getResultType(),
                settings.transform_null_in);

            DataTypes set_element_types = {in_first_argument->getResultType()};
            const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(set_element_types.front().get());
            if (left_tuple_type && left_tuple_type->getElements().size() != 1)
                set_element_types = left_tuple_type->getElements();

            for (auto & element_type : set_element_types)
                if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
                    element_type = low_cardinality_type->getDictionaryType();

            auto set_key = PreparedSetKey::forLiteral(in_second_argument->getTreeHash(), set_element_types);
            if (sets.getFuture(set_key))
                return;

            sets.addFromTuple(set_key, std::move(set), settings);

            //planner_context.registerSet(set_key, PlannerSet(FutureSet(std::move(set))));
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION ||
            in_second_argument_node_type == QueryTreeNodeType::TABLE)
        {
            auto set_key = PreparedSetKey::forSubquery(in_second_argument->getTreeHash());
            if (sets.getFuture(set_key))
                return;

            auto subquery_to_execute = in_second_argument;

            if (auto * table_node = in_second_argument->as<TableNode>())
            {
                auto storage_snapshot = table_node->getStorageSnapshot();
                auto columns_to_select = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));

                size_t columns_to_select_size = columns_to_select.size();

                auto column_nodes_to_select = std::make_shared<ListNode>();
                column_nodes_to_select->getNodes().reserve(columns_to_select_size);

                NamesAndTypes projection_columns;
                projection_columns.reserve(columns_to_select_size);

                for (auto & column : columns_to_select)
                {
                    column_nodes_to_select->getNodes().emplace_back(std::make_shared<ColumnNode>(column, subquery_to_execute));
                    projection_columns.emplace_back(column.name, column.type);
                }

                auto subquery_for_table = std::make_shared<QueryNode>(Context::createCopy(planner_context.getQueryContext()));
                subquery_for_table->setIsSubquery(true);
                subquery_for_table->getProjectionNode() = std::move(column_nodes_to_select);
                subquery_for_table->getJoinTree() = std::move(subquery_to_execute);
                subquery_for_table->resolveProjectionColumns(std::move(projection_columns));

                subquery_to_execute = std::move(subquery_for_table);
            }

            // auto subquery_options = select_query_options.subquery();
            // Planner subquery_planner(
            //     in_second_argument,
            //     subquery_options,
            //     planner_context.getGlobalPlannerContext());
            // subquery_planner.buildQueryPlanIfNeeded();

            // const auto & settings = planner_context.getQueryContext()->getSettingsRef();
            // SizeLimits size_limits_for_set = {settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
            // bool tranform_null_in = settings.transform_null_in;
            // auto set = std::make_shared<Set>(size_limits_for_set, false /*fill_set_elements*/, tranform_null_in);

            SubqueryForSet subquery_for_set;
            subquery_for_set.key = planner_context.createSetKey(in_second_argument);
            subquery_for_set.query_tree = std::move(subquery_to_execute);
            //subquery_for_set.source = std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan());

            /// TODO
            sets.addFromSubquery(set_key, std::move(subquery_for_set), settings, nullptr);

            //planner_context.registerSet(set_key, PlannerSet(in_second_argument));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function '{}' is supported only if second argument is constant or table expression",
                function_node->getFunctionName());
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    PlannerContext & planner_context;
    //const SelectQueryOptions & select_query_options;
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context) //, const SelectQueryOptions & select_query_options)
{
    CollectSetsVisitor visitor(planner_context); //, select_query_options);
    visitor.visit(node);
}

}
