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
#include <base/defines.h>
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


/// This is a wrapper function declared as an ActionsDAG's friend to access its private members.
/// The function is intended to perform DAG substitution for filter ActionsDAGs when it detects that the filter uses
/// some text search functions that are optimized to skip IO and text match operations.
///
/// The substitution is performed in this early optimization step because:
/// 1, We want to exclude the column information to read as soon as possible
/// 2. At some point we pretend to take advantage of lazy materialization
///
/// The optimization could be implemented in two variants: function-> function and function-> column We choose the first approach because in
/// the new function the main cost is the translation from roaring map to an index vector and for bug data sets the translation operation
/// may be expensive. So, the function -> function approach is more efficient and parallelizable.
///
/// For a query like:
/// select count() from table where hasToken(mytextcolumn, 'myword')
///
/// if: mytextcolumn has an associated text index called mytextcolumn_index
/// and: hasToken is an optimizable function according with: isOptimizableFunction
///
/// this class will replace some nodes in the dag (and the references to them) in order to create an equivalent query
///
/// select count() from table where _hasToken_index('mytextcolumn_index', 'myword', _part_index, _part_offset)
///
/// The new query will execute in a time fraction of the original one because:
/// 1. It does not require direct access to mytextcolumn but only to mytextcolumn_index (intended to be much smaller than the text column)
/// 2. With no access needed if can totally bypass the cost IO operations
/// 3. The text index was already read during the granules filter step
/// 4. Still uses all the parallelization infrastructure out of the box
///
/// The main entry function of this class are the constructor and the optimize function. All the other api is support functionality
/// to ensure that the replacement is possible and correct.
///
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach simplifies the work by avoiding
/// conflicts and minimizing coupling between this optimization and ActionsDAG.
class FunctionReplacerDAG
{

    ActionsDAG &dag;
    const std::map<String, String> &columns_to_index_name;

    /// These will be initialized in the constructor if they exist or initialized lazily in the first effective replacement to avoid
    /// redundant or repetitions in the node list.
    /// _part_index and _part_offset node pointers
    const ActionsDAG::Node * part_index_node;
    const ActionsDAG::Node * part_offset_node;

    /// Extract the context from the function. The functions need to store a reference to the context in order to be reemplazable with an
    /// index function. Only some special functions store a reference to the context, and the counterpart api (see:
    /// FunctionsStringSearchBase) was designed for that.
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

    /// If the column has an index associated in columns_to_index_name, try to get a column for it if already registered, or create a new
    /// one.  If there is no index for the column, then return nullptr
    const ActionsDAG::Node &tryGetOrCreateIndexColumn(const ActionsDAG::Node &column)
    {
        chassert(column.type == ActionsDAG::ActionType::INPUT);
        chassert(column.result_type->getTypeId() == TypeIndex::String);
        chassert(column.children.empty());

        const auto map_it = columns_to_index_name.find(column.result_name);
        chassert(map_it != columns_to_index_name.end());

        String name = fmt::format("'{}'_String", map_it->second);

        { /// Check if the associated index column already exist

            /// This uses backward iterators because the index column node (if exists already) should be inserted by a previous call of this
            /// same function.  Which uses dag.addColumn (push_back).
            const auto index_column_it = std::find_if(dag.nodes.rbegin(), dag.nodes.rend(),
                [&name](const ActionsDAG::Node &index_column) -> bool
                {
                    return index_column.result_name == name;
                });

            if (index_column_it != dag.nodes.rend())
                return *index_column_it;
        }

        /// else create it
        Field cast_type_constant_value(map_it->second);

        ColumnWithTypeAndName tmp;
        tmp.name = name;
        tmp.type = std::make_shared<DataTypeString>();
        tmp.column = tmp.type->createColumnConst(0, cast_type_constant_value);

        return dag.addColumn(tmp);
    }

    // Equivalent to tryFindInOutputs
    const ActionsDAG::Node * tryFindInInputs(const String & name) const
    {
        for (const auto & node : dag.inputs)
            if (node->result_name == name)
                return node;

        return nullptr;
    }

    /// Static helpers
    static bool isOptimizableFunction(const ActionsDAG::Node &subnode)
    {
        if (subnode.type != ActionsDAG::ActionType::FUNCTION || !subnode.function_base)
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

    bool isColumnNodeWithTextIndex(const ActionsDAG::Node *node) const
    {
        return (node->type == ActionsDAG::ActionType::INPUT
                && node->result_type->getTypeId() == TypeIndex::String
                && node->children.empty()
                && columns_to_index_name.contains(node->result_name));
    }

    bool hasChildColumnNodeWithTextIndex(const ActionsDAG::Node &subnode) const
    {
        return std::ranges::any_of(
            subnode.children,
            [&](const ActionsDAG::Node *child) -> bool { return isColumnNodeWithTextIndex(child); }
        );
    }

    /// Attempt to add a new node with the replacement function.
    /// This adds also extra input columns if needed.
    /// Returns the number of columns (inputs) replaced with an index name in the new function.
    int tryReplaceFunctionNodeInplace(ActionsDAG::Node &original_function_node)
    {
        chassert(isOptimizableFunction(original_function_node));
        const ContextPtr context = FunctionReplacerDAG::extractFunctionContext(original_function_node);

        // TODO: JAM This should go to the virtual function
        const String replacement_function_name = fmt::format("_{}_index", original_function_node.function->getName());

        /// At the moment I assume that the substitution function receives the same arguments than the original, but:
        /// 1. Substituting the indexed column with the index name (string column)
        /// 2. Adding two extra arguments, the virtual columns: _part_index and _part_offset
        /// TODO: If we can assume that the potential indexed column is always the first argument, or there will be only one,
        /// this code could be a bit simpler.
        ActionsDAG::NodeRawConstPtrs new_children;
        size_t replaced = 0;
        for (const auto *const child : original_function_node.children)
        {
            if (isColumnNodeWithTextIndex(child))
            {
                new_children.push_back(&tryGetOrCreateIndexColumn(*child));
                ++replaced; // count the replacements
                continue;
            }
            new_children.push_back(child);
        }

        /// When there was not replacement it means that none of the columns has a index associated in columns_to_index_name.
        /// In that case we don't perform any substitution
        if (replaced > 0)
        {
            /// The part_index_node and part_offset_node could be already inserted by some previous optimization or because the user query
            /// explicitly uses them. i.e: select _part_index, _part_offset where ...,
            /// or select ... where _part_index > X, or select ... where some_function(_part_offset)
            if (part_index_node == nullptr)
                part_index_node = &dag.addInput("_part_index", std::make_shared<DataTypeNumber<UInt64>>());
            new_children.push_back(part_index_node);

            if (part_offset_node == nullptr)
                part_offset_node = &dag.addInput("_part_offset", std::make_shared<DataTypeNumber<UInt64>>());
            new_children.push_back(part_offset_node);

            // From ActionsDAG::addFunction
            const ActionsDAG::Node & new_function_node = dag.addFunction(
                FunctionFactory::instance().get(replacement_function_name, context),
                new_children,
                original_function_node.result_name);

            original_function_node = new_function_node;

            // We just added it so, we expect it to be at the end to remove it.
            chassert(&new_function_node == &dag.nodes.back());
            dag.nodes.pop_back();
        }

        return replaced;
    }

public:

    explicit FunctionReplacerDAG(ActionsDAG &_dag, const std::map<String, String> &_columns_to_index_name)
        : dag(_dag), columns_to_index_name(_columns_to_index_name)
    {
        /// Check if these nodes are already there.
        /// This may happen if the query uses them explicitly.
        part_index_node = tryFindInInputs("_part_index");
        part_offset_node = tryFindInInputs("_part_offset");
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
    std::vector<String> optimize()
    {
        const std::vector<String> initial_inputs = dag.getRequiredColumnsNames();

        size_t replaced = 0;
        for (ActionsDAG::Node &subnode : dag.nodes)
        {
            if (!isOptimizableFunction(subnode))
                continue;

            if (!hasChildColumnNodeWithTextIndex(subnode))
                continue;

            replaced += tryReplaceFunctionNodeInplace(subnode);
        }

        if (replaced > 0)
        {
            dag.removeUnusedActions(true);

            const std::vector<String> final_inputs = dag.getRequiredColumnsNames();

            std::vector<String> removed_inputs;
            std::ranges::set_difference(initial_inputs, final_inputs, std::back_inserter(removed_inputs));

            chassert(removed_inputs.size() <= replaced);

            return removed_inputs;
        }

        return {};
    }
};

/// Text index search queries have this form:
///     SELECT [...]
///     FROM tab
///     WHERE text_function(text, criteria), [...]
/// where
/// - text_function is some of the text matching functions 'hasToken',
///    TODO: JAM add support for the others, but I will start with this one
///    TODO: JAM as a latter step we need to add AND and OR operators
/// - text is a column of text with a text index associated,
/// - criteria is the text criteria to search for
///    TODO: At the moment this is a single string, but that will change
///
/// This function replaces text function nodes from the user query (using semi-brute-force process) with optimized index variants that use
/// only the index information bypassing the column read (IO) + match which can consume more than 90% of execution time.
/// The optimization checks if the function's column node is a text column with an associated text index.
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

    constexpr size_t updated_layers = 0;

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step || node->children.size() != 1) // TODO: JAM This one will change when adding AND and OR support
        return updated_layers;

    node = node->children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return updated_layers;

    const StorageMetadataPtr metadata = read_from_mergetree_step->getStorageMetadata();
    if (!metadata || !metadata->hasSecondaryIndices())
        return updated_layers;

    /// Get the list of column: text_index we use latter
    std::map<String, String> columns_to_index_name;
    for (const IndexDescription &index : metadata->getSecondaryIndices())
    {
        if (index.type != "text")
            continue;

        chassert(index.column_names.size() == 1);
        columns_to_index_name.emplace(index.column_names.front(), index.name);
    }
    /// If no text index, the optimization doesn't apply, early exit
    if (columns_to_index_name.empty())
        return updated_layers;

    /// Look in detail is any of the inputs is a column with a text index, or early exit
    {
        bool has_mapped_input = false;
        for (const auto *const input: filter_step->getExpression().getInputs())
        {
            if (columns_to_index_name.contains(input->result_name))
            {
                has_mapped_input = true;
                break;
            }
        }
        if (!has_mapped_input)
            return updated_layers;
    }

    /// Ok, so far we can try to apply the optimization. Let's build a replaced.
    FunctionReplacerDAG replacer(filter_step->getExpression(), columns_to_index_name);

    const std::vector<String> removed_inputs = replacer.optimize();

    replacer.printDag();

    read_from_mergetree_step->registerColumnsChanges(
        removed_inputs,
        {"_part_index", "_part_offset"}
    );

    return 0;
}

}
