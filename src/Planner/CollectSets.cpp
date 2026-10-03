#include <Planner/CollectSets.h>

#include <Storages/StorageSet.h>

#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Set.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <unordered_set>


namespace DB
{
namespace Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsBool validate_enum_literals_in_operators;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
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
        if (const auto * table_node = node->as<TableFunctionNode>())
        {
            const auto & table_function_name = table_node->getTableFunctionName();
            const auto & context = planner_context.getQueryContext();
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().tryGet(table_function_name, context);
            auto skip_analysis_arguments_indexes = table_function_ptr->skipAnalysisForArguments(node, context);

            const auto & table_function_arguments = table_node->getArguments().getNodes();
            size_t table_function_arguments_size = table_function_arguments.size();

            for (size_t table_function_argument_index = 0; table_function_argument_index < table_function_arguments_size; ++table_function_argument_index)
            {
                const auto & table_function_argument = table_function_arguments[table_function_argument_index];

                auto skip_argument_index_it = std::find(skip_analysis_arguments_indexes.begin(), skip_analysis_arguments_indexes.end(), table_function_argument_index);
                if (skip_argument_index_it != skip_analysis_arguments_indexes.end())
                {
                    skip_children.insert(table_function_argument);
                    continue;
                }
            }
        }

        if (const auto * constant_node = node->as<ConstantNode>())
            /// Collect sets from source expression as well.
            /// Most likely we will not build them, but those sets could be requested during analysis.
            if (constant_node->hasSourceExpression())
                collectSets(constant_node->getSourceExpression(), planner_context);

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
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            if (sets.findStorage(set_key))
                return;
            auto ast = in_second_argument->toAST();
            sets.addFromStorage(set_key, std::move(ast), storage_set->getSet(), second_argument_table->getStorageID());
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = getSetElementsForConstantValue(
                in_first_argument->getResultType(), constant_node->getValue(), constant_node->getResultType(),
                GetSetElementParams{
                    .transform_null_in = settings[Setting::transform_null_in],
                    .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
                });

            if (set.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' second argument evaluated to Block with no columns",
                    function_node->getFunctionName());

            DataTypes set_element_types;
            set_element_types.reserve(set.size());
            /// Get the `set_element_types` from `set` instead of `in_first_argument` because
            /// inside `getSetElementsForConstantValue`, we already do necessary transformation including
            /// getting `dictionaryType` from `DataTypeLowCardinality`. Therefore, we can skip some steps here if
            /// we directly use `set` to get the `set_element_types`.
            for (const auto & elem : set)
                set_element_types.push_back(elem.type);

            set_element_types = Set::getElementTypes(std::move(set_element_types), settings[Setting::transform_null_in]);
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});

            if (sets.findTuple(set_key, set_element_types))
                return;

            auto ast = in_second_argument->toAST();
            sets.addFromTuple(set_key, std::move(ast), std::move(set), settings);
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION ||
            in_second_argument_node_type == QueryTreeNodeType::TABLE)
        {
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            if (sets.findSubquery(set_key))
                return;

            auto subquery_to_execute = in_second_argument;
            if (in_second_argument->as<TableNode>())
                subquery_to_execute = buildSubqueryToReadColumnsFromTableExpression(subquery_to_execute, planner_context.getQueryContext());

            auto ast = in_second_argument->toAST({ .set_subquery_cte_name = false });
            sets.addFromSubquery(set_key, std::move(ast), std::move(subquery_to_execute), settings);
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function '{}' is supported only if second argument is constant or table expression",
                function_node->getFunctionName());
        }
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        if (skip_children.contains(child_node))
        {
            skip_children.erase(child_node);
            return false;
        }

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    PlannerContext & planner_context;
    std::unordered_set<QueryTreeNodePtr> skip_children;
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context)
{
    CollectSetsVisitor visitor(planner_context);
    visitor.visit(node);
}

}
