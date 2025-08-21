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
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Functions/FunctionFactory.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsStringSearch.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <__format/format_functions.h>
#include <base/defines.h>
#include <algorithm>
#include <memory>
#include <format>

#include <cstdio>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct IndexInfo : public IndexSize
{
    String name;

    IndexInfo(String name_, IndexSize index_size)
        : IndexSize(index_size)
        , name(name_)
    {
    }
};

std::map<String, IndexInfo> getIndexInfosForColumns(const ReadFromMergeTree * read_from_mergetree_step)
{
    std::map<String, IndexInfo> columns_to_index_infos;

    const StorageMetadataPtr metadata = read_from_mergetree_step->getStorageMetadata();
    if (!metadata || !metadata->hasSecondaryIndices())
        return {};

    const IStorage::IndexSizeByName secondary_index_sizes = read_from_mergetree_step->getMergeTreeData().getSecondaryIndexSizes();

    /// Get the list of columns: text_index we use latter and construct the size information needed by Replacer
    for (const auto & index_description : metadata->getSecondaryIndices())
    {
        if (index_description.type != "text")
            continue;

        auto size_it = secondary_index_sizes.find(index_description.name);
        if (size_it == secondary_index_sizes.end())
            continue;

        /// Poor man's detection if the index is not materialized.
        /// TODO This needs more work because the materialization is per part but here we check per column.
        if (size_it->second.marks == 0 || size_it->second.data_uncompressed == 0)
            continue;

        chassert(index_description.column_names.size() == 1);
        columns_to_index_infos.emplace(index_description.column_names.front(), IndexInfo(index_description.name, size_it->second));
    }

    return columns_to_index_infos;
}

}

/// This class substitutes filters with text-search functions by new internal functions which skip IO and read less data.
///
/// The substitution is performed in this early optimization step because:
/// 1, We want to exclude the column information to read as soon as possible
/// 2. At some point we pretend to take advantage of lazy materialization
///
/// The optimization could be implemented in two variants: function -> function and function -> column. We choose the first approach because
/// in the new function the main cost is the decompression of the compressed posting lists (currently stored as roaring bitmap). So, the
/// function -> function approach is more efficient and parallelizable.
///
/// For a query like
///     SELECT count() FROM table WHERE hasToken(text_col, 'token')
/// if 1) text_col has an associated text index called text_col_idx, and 2) hasToken is an optimizable function (according to
/// isOptimizableFunction), then this class replaces some nodes in the ActionsDAG (and references to them) to generate an
/// equivalent query
///     SELECT count() FROM table where _hasToken_index('text_col_idx', 'token', _part_index, _part_offset)
///
/// The new query will execute a lot faster the original one because:
/// 1. It does not require direct access to text_col but only to text_col_idx (intended to be much smaller than the text column)
/// 2. With no access needed if can totally bypass the cost IO operations
/// 3. The text index was already read during the granules filter step
/// 4. Still uses all the parallelization infrastructure out of the box
///
/// The main entry point of this class are the constructor and the replace function. All the other api is support functionality
/// to ensure that the replacement is possible and correct.
///
/// This class is a (C++) friend of ActionsDAG and can therefore access its private members.
///
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach simplifies the work by avoiding
/// conflicts and minimizing coupling between this optimization and ActionsDAG.
class FullTextMatchingFunctionDAGReplacer
{
public:
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const std::map<String, IndexInfo> & columns_to_index_info_)
        : actions_dag(actions_dag_)
        , columns_to_index_info(columns_to_index_info_)
    {
        /// Check if these nodes are already there.
        /// This may happen if the query uses them explicitly.
        part_index_node = tryFindInInputs("_part_index");
        part_offset_node = tryFindInInputs("_part_offset");
    }

    /// This optimization replaces text-search functions by internal functions.
    /// Example: hasToken(text_col, 'token') -> hasToken('text_col_idx', 'token', _part_index, _part_offset)
    ///
    /// In detail:
    /// 0. Insert a new node with the replacement function, including extra column nodes.
    /// 1. Remove the `text_col` column input + node if it is no longer referenced after substitution.
    /// 2. Update all references to the old function node.
    /// 3. Remove the old function node.
    /// 4. Update the output references if needed.
    ///
    /// TODO: A much simpler alternative would be to substitute the node inplace (update it's internal information).
    /// Such approach avoids the latter redirection step. However, ActionDAG::addFunctionImpl is too complex for that ...
    /// We could modify ::addFunctionImpl, but at the moment I prefer not to touch any code in ActionDAG.
    std::vector<String> replace()
    {
        const std::vector<String> initial_inputs = actions_dag.getRequiredColumnsNames();

        size_t replaced = 0;
        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            if (!isOptimizableFunction(node))
                continue;

            if (!hasChildColumnNodeWithTextIndex(node))
                continue;

            replaced += tryReplaceFunctionNodeInplace(node);
        }
        if (replaced == 0)
            return {};

        actions_dag.removeUnusedActions(true);

        const std::vector<String> final_inputs = actions_dag.getRequiredColumnsNames();

        std::vector<String> removed_inputs;
        std::ranges::set_difference(initial_inputs, final_inputs, std::back_inserter(removed_inputs));

        chassert(removed_inputs.size() <= replaced);

        return removed_inputs;
    }

    void printDag() const
    {
        for (int i = 0; const ActionsDAG::Node & node : actions_dag.getNodes())
        {
            std::println("\nNode {}: {}", i++, node.result_name);
            printHierarchicalActions(& node, 0);
        }
        for (int i = 0; const ActionsDAG::Node *input : actions_dag.getInputs())
        {
            std::println("\nInput {}: {}", i++, input->result_name);
            printHierarchicalActions(input, 0);
        }
        for (int i = 0; const ActionsDAG::Node *output : actions_dag.getOutputs())
        {
            std::println("\nOutput {}: {}", i++, output->result_name);
            printHierarchicalActions(output, 0);
        }
    }

private:
    ActionsDAG & actions_dag;
    const std::map<String, IndexInfo> & columns_to_index_info;

    /// These will be initialized in the constructor if they exist or initialized lazily in the first effective replacement to avoid
    /// redundant or repetitions in the node list.
    /// _part_index and _part_offset node pointers
    const ActionsDAG::Node * part_index_node;
    const ActionsDAG::Node * part_offset_node;

    /// Extract the context from the function. The functions need to store a reference to the context in order to be reemplazable with an
    /// index function. Only some special functions store a reference to the context, and the counterpart api (see:
    /// FunctionsStringSearchBase) was designed for that.
    static ContextPtr extractFunctionContext(const ActionsDAG::Node & original_function_node)
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

    /// If the column has an index associated in columns_to_index_name, try to get a column for it if already registered, or create a new
    /// one.  If there is no index for the column, then return nullptr
    const ActionsDAG::Node & tryGetOrCreateIndexColumn(const ActionsDAG::Node & column)
    {
        chassert(column.type == ActionsDAG::ActionType::INPUT);
        chassert(column.result_type->getTypeId() == TypeIndex::String);
        chassert(column.children.empty());

        const auto map_it = columns_to_index_info.find(column.result_name);
        chassert(map_it != columns_to_index_info.end());

        String name = std::format("'{}'_String", map_it->second.name);

        {
            /// Check if the associated index column already exist

            /// This uses backward iterators because the index column node (if exists already) should be inserted by a previous call of this
            /// same function.  Which uses actions_dag.addColumn (push_back).
            const auto index_column_it = std::find_if(actions_dag.nodes.rbegin(), actions_dag.nodes.rend(),
                [&name](const ActionsDAG::Node & index_column)
                {
                    return index_column.result_name == name;
                });

            if (index_column_it != actions_dag.nodes.rend())
                return *index_column_it;
        }

        /// else create it
        Field cast_type_constant_value(map_it->second.name);

        ColumnWithTypeAndName tmp;
        tmp.name = name;
        tmp.type = std::make_shared<DataTypeString>();
        tmp.column = tmp.type->createColumnConst(0, cast_type_constant_value);

        return actions_dag.addColumn(tmp);
    }

    /// Works similar as ActionsDAG::tryFindInOutputs
    const ActionsDAG::Node * tryFindInInputs(const String & name) const
    {
        for (const auto & node : actions_dag.inputs)
            if (node->result_name == name)
                return node;

        return nullptr;
    }

    static bool isOptimizableFunction(const ActionsDAG::Node &subnode)
    {
        if (subnode.type != ActionsDAG::ActionType::FUNCTION || !subnode.function_base)
            return false;

        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(subnode.function_base.get());
        chassert(adaptor);
        const auto function = std::dynamic_pointer_cast<FunctionsStringSearchBase>(adaptor->getFunction());
        if (function == nullptr || function->info != FunctionsStringSearchBase::Info::Optimizable)
            return false;

        chassert(function->getContext() != nullptr);

        return true;
    }

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

    bool isColumnNodeWithTextIndex(const ActionsDAG::Node * node) const
    {
        return (node->type == ActionsDAG::ActionType::INPUT
                && node->result_type->getTypeId() == TypeIndex::String
                && node->children.empty()
                && columns_to_index_info.contains(node->result_name));
    }

    bool hasChildColumnNodeWithTextIndex(const ActionsDAG::Node & subnode) const
    {
        return std::ranges::any_of(
            subnode.children,
            [&](const ActionsDAG::Node * child) { return isColumnNodeWithTextIndex(child); }
        );
    }

    /// Attempt to add a new node with the replacement function.
    /// This also adds extra input columns if needed.
    /// Returns the number of columns (inputs) replaced with an index name in the new function.
    int tryReplaceFunctionNodeInplace(ActionsDAG::Node & original_function_node)
    {
        chassert(isOptimizableFunction(original_function_node));
        const ContextPtr context = FullTextMatchingFunctionDAGReplacer::extractFunctionContext(original_function_node);

        /// TODO: This should go to the virtual function
        const String replacement_function_name = std::format("_{}_index", original_function_node.function->getName());

        /// At the moment I assume that the substitution function receives the same arguments than the original, but:
        /// 1. Substituting the indexed column with the index name (string column)
        /// 2. Adding two extra arguments, the virtual columns: _part_index and _part_offset
        /// TODO: If we can assume that the potential indexed column is always the first argument, or there will be only one,
        /// this code could be a bit simpler.
        ActionsDAG::NodeRawConstPtrs new_children;
        size_t replaced = 0;
        for (const auto * const child : original_function_node.children)
        {
            if (isColumnNodeWithTextIndex(child))
            {
                new_children.push_back(&tryGetOrCreateIndexColumn(*child));
                ++replaced; /// count the replacements
                continue;
            }
            new_children.push_back(child);
        }

        /// If there was not replacement, it means that none of the columns has a index associated in columns_to_index_info.
        /// In that case we don't perform any substitution.
        if (replaced > 0)
        {
            /// The part_index_node and part_offset_node could be already inserted by some previous optimization or because the user query
            /// explicitly uses them. i.e: select _part_index, _part_offset where ...,
            /// or select ... where _part_index > X, or select ... where some_function(_part_offset)
            if (part_index_node == nullptr)
                part_index_node = &actions_dag.addInput("_part_index", std::make_shared<DataTypeNumber<UInt64>>());
            new_children.push_back(part_index_node);

            if (part_offset_node == nullptr)
                part_offset_node = &actions_dag.addInput("_part_offset", std::make_shared<DataTypeNumber<UInt64>>());
            new_children.push_back(part_offset_node);

            /// From ActionsDAG::addFunction
            const ActionsDAG::Node & new_function_node = actions_dag.addFunction(
                FunctionFactory::instance().get(replacement_function_name, context),
                new_children,
                original_function_node.result_name);

            original_function_node = new_function_node;

            /// We just added it so, we expect it to be at the end to remove it.
            chassert(&new_function_node == &actions_dag.nodes.back());
            actions_dag.nodes.pop_back();
        }

        return replaced;
    }
};

/// Text index search queries have this form:
///     SELECT [...]
///     FROM tab
///     WHERE text_function(...), [...]
/// where
/// - text_function is a text-matching functions, e.g. 'hasToken',
///   text-matching functions expect that the column on which the function is called has a text index
/// TODO: add support for other function, before that support AND and OR operators
///
/// This function replaces text function nodes from the user query (using semi-brute-force process) with internal functions which use only
/// the index information to bypass the normal column scan (read step) which can consume more than 90% of execution time. The optimization
/// checks if the function's column node is a text column with a text index.
///
/// (*) Text search only makes sense if a text index exists on text. In the scope of this function, we don't care.
///     That check is left to query runtime, ReadFromMergeTree specifically.
size_t tryDirectReadFromTextIndex(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings &)
{
    QueryPlan::Node * node = parent_node;

    /// Expect this query plan:
    /// FilterStep
    ///    ^
    ///    |
    /// ReadFromMergeTree

    constexpr size_t no_layers_updated = 0;

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step || node->children.size() != 1) // TODO: This one will change when adding AND and OR support
        return no_layers_updated;

    node = node->children.front();
    ReadFromMergeTree * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return no_layers_updated;

    /// Get information of MATERIALIZED indices
    std::map<String, IndexInfo> columns_to_index_info = getIndexInfosForColumns(read_from_mergetree_step);
    if (columns_to_index_info.empty())
        return no_layers_updated;

    /// If the expression contains no columns with a text index as input, do nothing.
    bool has_input_has_column_with_text_index = false;
    for (const auto * const input : filter_step->getExpression().getInputs())
    {
        const auto index_it = columns_to_index_info.find(input->result_name);
        if (index_it == columns_to_index_info.end())
            continue;

        has_input_has_column_with_text_index = true;
        break;
    }
    if (!has_input_has_column_with_text_index)
        return no_layers_updated;

    /// Now try to modify the ActionsDAG.
    FullTextMatchingFunctionDAGReplacer replacer(filter_step->getExpression(), columns_to_index_info);

    const std::vector<String> removed_inputs = replacer.replace();

    /// replacer.printDag();

    read_from_mergetree_step->registerColumnsChanges(removed_inputs, {"_part_index", "_part_offset"});

    return 0;
}

}
