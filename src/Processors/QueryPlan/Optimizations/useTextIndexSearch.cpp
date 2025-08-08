#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Functions/FunctionFactory.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsStringSearch.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <__format/format_functions.h>
#include "base/defines.h"
#include <algorithm>
#include <memory>
#include <format>

#include <cstdio>

namespace DB::QueryPlanOptimizations
{

static void printHierarchicalActions(const ActionsDAG::Node * node, int indent)
{
    if (indent == 0)
        std::println("=== Node: {} ===", node->result_name);
    else
        for (int i = 0; i < 3 * indent; ++i)
            std::print(" ");

    std::print("{} ", static_cast<const void*>(node));

    if (node->function_base)
        std::print("BaseFunc: {} -> ", node->function_base->getName());

    if (node->function)
        std::print("ExecutableFunc: {} -> ", node->function->getName());

    std::print(" result {} (compiled: {}) (type: {}) (node type {})",
        node->result_name,
        node->is_function_compiled,
        node->result_type->getName(),
        static_cast<int>(node->type)
    );

    if (node->column)
        std::print(" column {} (id: {})",
            node->column->getName(),
            static_cast<int>(node->column->getDataType())
        );

    std::println("");

    for (const ActionsDAG::Node * subnode : node->children)
        printHierarchicalActions(subnode, indent + 1);
}


class FunctionReplacerDAG {

    ActionsDAG &dag;
    const std::map<std::string, std::string> &map_indexed_columns;
    std::vector<std::string> removed_columns;

    // Check if these nodes are already there.
    const ActionsDAG::Node * pindex;
    const ActionsDAG::Node * poffsets;

    /// Extract the context from the function. The functions need to store a reference to the context in order to be reemplazable with an
    /// index function.
    static ContextPtr extractFunctionContext(const ActionsDAG::Node &original_function_node)
    {
        chassert(original_function_node.type == ActionsDAG::ActionType::FUNCTION);
        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(original_function_node.function_base.get());
        chassert(adaptor);
        const auto function = std::dynamic_pointer_cast<FunctionsStringSearchBase>(adaptor->getFunction());
        chassert(function != nullptr);
        ContextPtr context = function->getContext();
        chassert(context != nullptr);
        return context;
    }

    /// If the column has an index associated in map_indexed_columns, try to get a column for it if already registered, or create a new one.
    /// Is there is no index for the column, then return nullptr
    const ActionsDAG::Node &tryGetOrCreateIndexColumn(const ActionsDAG::Node &column)
    {
        chassert(column.type == ActionsDAG::ActionType::INPUT);
        chassert(column.result_type->getTypeId() == TypeIndex::String);
        chassert(column.children.empty());

        const auto map_it = map_indexed_columns.find(column.result_name);
        chassert(map_it != map_indexed_columns.end());

        { /// Check if the associated index column already exist
            const auto index_column_it = std::find_if(dag.nodes.rbegin(), dag.nodes.rend(),
                [&map_it](const ActionsDAG::Node &index_column) -> bool
                {
                    return index_column.result_name == map_it->second;
                });

            if (index_column_it != dag.nodes.rend())
                return *index_column_it;
        }

        /// else create it
        Field cast_type_constant_value(map_it->second);

        ColumnWithTypeAndName tmp;
        tmp.name = std::format("'{}'_String", map_it->second);
        tmp.type = std::make_shared<DataTypeString>();
        tmp.column = tmp.type->createColumnConst(0, cast_type_constant_value);

        return dag.addColumn(tmp);
    }

    const ActionsDAG::Node * tryFindInInputs(const std::string & name) const
    {
        for (const auto & node : dag.inputs)
            if (node->result_name == name)
                return node;

        return nullptr;
    }

    /// Static helpers
    static bool isOptimizableFunction(const ActionsDAG::Node &subnode)
    {
        if (subnode.type != ActionsDAG::ActionType::FUNCTION || !subnode.function)
            return false;

        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(subnode.function_base.get());
        chassert(adaptor);
        const auto function = std::dynamic_pointer_cast<FunctionsStringSearchBase>(adaptor->getFunction());
        if (function == nullptr)
            return false;
        if (function->getContext() == nullptr)
            return false;

        return true;
    }

    bool isIndexedColumnNode(const ActionsDAG::Node *node) const
    {
        return (node->type == ActionsDAG::ActionType::INPUT
                && node->result_type->getTypeId() == TypeIndex::String
                && node->children.empty()
                && map_indexed_columns.contains(node->result_name));
    }

    bool hasIndexedChild(const ActionsDAG::Node &subnode) const
    {
        return std::ranges::any_of(
            subnode.children,
            [&](const ActionsDAG::Node *child) -> bool { return isIndexedColumnNode(child); }
        );
    }

    /// Attempt to add a new node with the replacement function.
    /// This adds also extra input columns if needed.
    /// Returns a pointer to the new node if added or nullptr otherwise
    const ActionsDAG::Node * tryAddReplacementIndexFunction(const ActionsDAG::Node &original_function_node)
    {
        chassert(isOptimizableFunction(original_function_node));
        const ContextPtr context = FunctionReplacerDAG::extractFunctionContext(original_function_node);

        // TODO: JAM This should go to the virtual function
        const std::string replacement_function_name = std::format("{}Index", original_function_node.function->getName());

        size_t replaced = 0;
        /// At the moment I assume that the substitution function receives the same arguments than the original, but:
        /// 1. Substituting the indexed column with the index name (string column)
        /// 2. Adding two extra arguments, the virtual columns: _part_index and _part_offset
        ActionsDAG::NodeRawConstPtrs new_children;
        new_children.reserve(original_function_node.children.size() + 2);
        std::ranges::transform(
            original_function_node.children,
            std::back_inserter(new_children),
            [&](const ActionsDAG::Node *child) -> const ActionsDAG::Node *
            {
                if (!isIndexedColumnNode(child))
                    return child;

                ++replaced; // count the replacements
                return &tryGetOrCreateIndexColumn(*child);
            });

        /// When there was not replacement it means that none of the columns has a index associated in map_indexed_columns
        /// In that case we don't perform any substitution
        if (replaced == 0)
            return nullptr;

        if (pindex == nullptr)
            pindex = &dag.addInput("_part_index", std::make_shared<DataTypeNumber<UInt64>>());
        new_children.push_back(pindex);

        if (poffsets == nullptr)
            poffsets = &dag.addInput("_part_offset", std::make_shared<DataTypeNumber<UInt64>>());
        new_children.push_back(poffsets);

        return &dag.addFunction(
            FunctionFactory::instance().get(replacement_function_name, context),
            new_children,
            original_function_node.result_name
        );
    }

    size_t replaceFunctionNode(const ActionsDAG::Node &function_node, const ActionsDAG::Node &replacement_node)
    {
        chassert(function_node.type == ActionsDAG::ActionType::FUNCTION);

        std::list<ActionsDAG::Node>::iterator function_it = dag.nodes.end();

        /// Counter for the children nodes referenced by other nodes.
        std::map<const ActionsDAG::Node *, size_t> column_references_map;
        for (const ActionsDAG::Node * node : function_node.children)
        {
            if (node->type != ActionsDAG::ActionType::INPUT) /// TODO: JAM This is too restrictive, but fine for development.
                continue;

            auto [_, inserted] = column_references_map.emplace(node, 0);
            chassert(inserted);
        }

        /// 3 things in one iteration
        /// 1. Count the references to the function's children nodes
        /// 2. get the iterator to the function node.
        /// 3. Redirect parent references to function_node -> replacement_node
        for (ActionsDAG::Nodes::iterator it = dag.nodes.begin(); it != dag.nodes.end(); ++it)
        {
            if (&*it == &function_node) {
                function_it = it;
                continue;           // continue here to not count this references.
            }

            for (auto &child : it->children)
            {
                if (child->type == ActionsDAG::ActionType::INPUT)
                {
                    /// Check other references to my children
                    auto map_it = column_references_map.find(child);
                    if (map_it != column_references_map.end())
                        map_it->second++;

                }
                else if (child->type == ActionsDAG::ActionType::FUNCTION)
                {
                    /// redirect references in other nodes function_node -> replacement_node;
                    if (child == &function_node)
                        child = &replacement_node;
                }
            }
        }
        chassert(function_it != dag.nodes.end());

        std::println("============================ CALLED BEFORE ============================");
        printDag();
        std::println("===============================================================");

        /// Remove unused inputs
        {
            auto [it_end, last] = std::ranges::remove_if(
                dag.inputs,
                [&column_references_map](const ActionsDAG::Node *input) -> bool
                {
                    return column_references_map.contains(input) && column_references_map.at(input) == 0;
                });
            dag.inputs.erase(it_end, last);
        }

        // replace outputs
        std::ranges::replace(dag.outputs, &function_node, &replacement_node);

        /// Remove the old node
        dag.nodes.erase(function_it);

        // Now erase the non-referenced nodes
        return dag.nodes.remove_if(
            [&](const ActionsDAG::Node &node) -> bool
            {
                if (node.type == ActionsDAG::ActionType::INPUT
                    && column_references_map.contains(&node)
                    && column_references_map.at(&node) == 0)
                {
                    chassert(node.children.empty());
                    removed_columns.push_back(node.result_name);
                    return true;
                }
                return false;
            });
    }

public:

    explicit FunctionReplacerDAG(ActionsDAG &_dag, const std::map<std::string, std::string> &_map_indexed_columns)
        : dag(_dag), map_indexed_columns(_map_indexed_columns)
    {
        /// Check if these nodes are already there.
        pindex = tryFindInInputs("_part_index");
        poffsets = tryFindInInputs("_part_offset");
    }


    void printDag() const
    {
        int i = 0;
        for (const ActionsDAG::Node &subnode : dag.getNodes())
        {
            std::println("\nNode {}: {}", i++, subnode.result_name);
            printHierarchicalActions(&subnode, 0);
        }
        i = 0;
        for (const ActionsDAG::Node *input : dag.getInputs())
        {
            std::println("\nInput {}: {}", i++, input->result_name);
            printHierarchicalActions(input, 0);
        }
        i = 0;
        for (const ActionsDAG::Node *output : dag.getOutputs())
        {
            std::println("\nOutput {}: {}", i++, output->result_name);
            printHierarchicalActions(output, 0);
        }

    }

    /// This optimization performs a replacement of the text search functions into pure index functions
    /// i.e hasToken(text_column, "token") -> hasToken("text_column_index", "token", _part_index, _part_offset)
    /// As expected, the replacement implies:
    /// 0. Insert a new node with the replacement function (including also the extra column nodes)
    /// 1. Remove the `text_column` column input+node when if not referenced after the substitution.
    /// 2. Update all the references to the old function node.
    /// 3. Remove the old function node
    /// 4. Update the output references if needed.
    /// The hardest part of the work is in: replaceFunctionNode
    /// TODO: JAM Ideally a much simpler alternative would be to substitute inplace the node (update it's internal information)
    /// Such approach avoids the latter redirection step. However, the addFunctionImpl api is so complex that de-incentivated doing that
    /// We could modify that function to allow doing that, but at the moment I prefer not to touch any code in that side until this is
    /// accepted
    size_t optimize()
    {
        size_t replaced = 0;
        for (const ActionsDAG::Node &subnode : dag.getNodes())
        {
            if (!isOptimizableFunction(subnode))
                continue;

            if (!hasIndexedChild(subnode))
                continue;

            const ActionsDAG::Node * replacement = tryAddReplacementIndexFunction(subnode);

            if (replacement != nullptr)
                replaced += replaceFunctionNode(subnode, *replacement);
        }

        return replaced;
    }

    const std::vector<std::string> &removedColumns() const
    {
        return removed_columns;
    }
};

/// Invert index text search queries have this form:
///     SELECT [...]
///     FROM tab, [...]
///     WHERE text_function(text, criteria), [...]
/// where
/// - text_function is some of the text matching functions 'hasToken',
///    TODO: JAM add support fot the others, but I will start with this one
///    TODO: JAM as a latter step we need to add AND and OR operators
/// - text is a column of text with a text index associated,
/// - criteria is the text criteria to search for
///    TODO: At the moment this is a single string, but that will change
///
/// This function extracts text_function, text, and criteria from the query plan without rewriting it.
/// The extracted values are then passed to ReadFromMergeTree which can then use the vector similarity index
/// to speed up the search.
///
/// (*) Text search only makes sense if a text index exists on text. In the scope of this function, we don't care.
///     That check is left to query runtime, ReadFromMergeTree specifically.
size_t tryUseTextSearch(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & )
{
    QueryPlan::Node * node = parent_node;

    /// Expect this query plan:
    /// FilterStep
    ///    ^
    ///    |
    /// ReadFromMergeTree

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step || node->children.size() != 1) // TODO: JAM This one will change when adding AND and OR support
        return 0;

    node = node->children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return 0;

    const StorageMetadataPtr metadata = read_from_mergetree_step->getStorageMetadata();
    if (!metadata)
        return 0;

    // Only makes sense if there is a text index in the table.
    const IndicesDescription& all_indexes = metadata->getSecondaryIndices();
    if (all_indexes.empty() || !all_indexes.hasType("text"))
        return 0;

    /// Now check if the table has a text index
    std::map<std::string, std::string> map_indexed_columns;
    for (const IndexDescription &index : all_indexes)
    {
        if (index.type != "text")
            continue;

        chassert(index.column_names.size() == 1);
        map_indexed_columns.emplace(index.column_names.front(), index.name);
    }
    /// If no text index, the optimization doesn't apply, early exit
    if (map_indexed_columns.empty())
        return 0;

    /// Look in detail is any of the inputs is a column with a text index, or early exit
    const bool has_mapped_input = std::ranges::any_of(
        filter_step->getExpression().getInputs(),
        [&map_indexed_columns](const ActionsDAG::Node *input) -> bool
        {return map_indexed_columns.contains(input->result_name);});
    if (!has_mapped_input)
        return 0;

    /// Ok, so far we can try to apply the optimization. Let's build a replaced.
    FunctionReplacerDAG replacer(filter_step->getExpression(), map_indexed_columns);
    std::println("============================ BEFORE ============================");
    replacer.printDag();

    size_t replaced = replacer.optimize();
    if (replaced == 0)
        return 0;

    std::println("============================ AFTER ============================");
    replacer.printDag();
    std::println("===============================================================");

    read_from_mergetree_step->registerColumnsChanges(
        replacer.removedColumns(),
        {"_part_index", "_part_offset"}
    );

    return 0;
}

}
