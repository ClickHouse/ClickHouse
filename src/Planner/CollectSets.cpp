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
    explicit CollectSetsVisitor(PlannerContext & planner_context_)
        : planner_context(planner_context_)
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
        auto & sets = planner_context.getPreparedSets();

        /// Tables and table functions are replaced with subquery at Analysis stage, except special Set table.
        auto * second_argument_table = in_second_argument->as<TableNode>();
        StorageSet * storage_set = second_argument_table != nullptr ? dynamic_cast<StorageSet *>(second_argument_table->getStorage().get()) : nullptr;

        if (storage_set)
        {
            /// Handle storage_set as ready set.
            auto set_key = in_second_argument->getTreeHash();
            sets.addFromStorage(set_key, storage_set->getSet());
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = getSetElementsForConstantValue(
                in_first_argument->getResultType(),
                constant_node->getValue(),
                constant_node->getResultType(),
                settings.transform_null_in);

            DataTypes set_element_types = {in_first_argument->getResultType()};
            const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(set_element_types.front().get());
            if (left_tuple_type && left_tuple_type->getElements().size() != 1)
                set_element_types = left_tuple_type->getElements();

            set_element_types = Set::getElementTypes(std::move(set_element_types), settings.transform_null_in);
            auto set_key = in_second_argument->getTreeHash();

            if (sets.findTuple(set_key, set_element_types))
                return;

            sets.addFromTuple(set_key, std::move(set), settings);
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION ||
            in_second_argument_node_type == QueryTreeNodeType::TABLE)
        {
            auto set_key = in_second_argument->getTreeHash();
            if (sets.findSubquery(set_key))
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

            sets.addFromSubquery(set_key, std::move(subquery_to_execute), settings);
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
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context)
{
    CollectSetsVisitor visitor(planner_context);
    visitor.visit(node);
}

}
