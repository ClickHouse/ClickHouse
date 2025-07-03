#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeIndices.h>


#include <Functions/IFunctionAdaptors.h>


#include <cstdio>

namespace DB::QueryPlanOptimizations
{

static void printHierarchicalDAG(const QueryPlan::Node * parent_node, int indent)
{
    const QueryPlan::Node * node = parent_node;

    if (indent == 0)
        printf("\n=== Optimize Tree ===\n");

    for (int i = 0; i < indent; ++i)
        printf(" ");

    printf("%s\n", node->step->getName().c_str());
    for (QueryPlan::Node * subnode : node->children)
        printHierarchicalDAG(subnode, indent + 1);
}

static void printHierarchicalActions(const ActionsDAG::Node * node, int indent)
{
    if (indent == 0)
        printf("=== SUBTREE ===\n");

    for (int i = 0; i < indent; ++i)
        printf(" ");

    printf("(%p) ", reinterpret_cast<const void *>(node));

    if (node->function_base)
        printf("BaseFunc: %s -> ", node->function_base->getName().c_str());

    if (node->function)
        printf("ExecutableFunc: %s -> ", node->function->getName().c_str());

    printf(" result %s (compiled: %d) (type: %s) (node type %d)",
        node->result_name.c_str(),
        node->is_function_compiled,
        node->result_type->getName().c_str(),
        static_cast<int>(node->type)
    );

    if (node->column)
        printf(" column %s (id: %d)",
            node->column->getName().c_str(),
            static_cast<int>(node->column->getDataType())
        );

    printf("\n");

    for (const ActionsDAG::Node * subnode : node->children)
        printHierarchicalActions(subnode, indent + 1);
}

/// Utility function analyzes the subDag and searches for the function entries
/// This is intended to be extended to support more functions in the future.
/// Returns a multimap like:
/// { column1: {token1, token2, token3},
///   column2: {token4, token5, token6} }
static std::multimap<std::string, std::string> extractTextSearches(ActionsDAG &subDAG)
{
    std::multimap<std::string, std::string> result;
    for (const ActionsDAG::Node &subnode : subDAG.getNodes())
    {
        if (subnode.type != ActionsDAG::ActionType::FUNCTION
            || !subnode.function
            || subnode.function->getName() != "hasToken")
            continue;

        chassert(subnode.result_name.starts_with("hasToken")); // Yes I am paranoid ;)
        chassert(subnode.children.size() == 2);

        const ActionsDAG::Node *column = subnode.children.at(0);
        const ActionsDAG::Node *text = subnode.children.at(1);

        // Check that subnodes have the right type (defensive for substitution)
        // Check not only the types, but also that the node does not have any "children"
        // in order to substitute safely
        chassert(column->type == ActionsDAG::ActionType::INPUT);
        chassert(column->result_type->getTypeId() == TypeIndex::String);
        chassert(column->children.empty());

        chassert(text->type == ActionsDAG::ActionType::COLUMN);
        chassert(text->result_type->getTypeId() == TypeIndex::String);
        chassert(text->children.empty());
        chassert(text->column);
        chassert(text->column->getDataType() == TypeIndex::String);

        result.emplace(column->result_name, text->result_name);
    }

    return result;
}

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

    printHierarchicalDAG(node, 0);
    // bool additional_filters_present = false; /// WHERE or PREWHERE

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


    const ActionsDAG::Nodes & filter_nodes = filter_step->getExpression().getNodes();

    //auto & expression = filter_step->getExpression();
    const auto & filter_column_name = filter_step->getFilterColumnName();
    printf(" Column FILTER: %s\n", filter_column_name.c_str());


    printf("Filter subnodes: %zu\n", filter_nodes.size());

    size_t i = 0;
    for (const ActionsDAG::Node &subnode : filter_nodes)
    {
        printf("\nNode %zu\n", i++);
        printHierarchicalActions(&subnode, 0);
    }


    std::multimap<std::string, std::string> text_searches_map = extractTextSearches(filter_step->getExpression());
    if (text_searches_map.empty())
        return 0;

    std::map<std::string, std::string> map_column_index;
    for (const IndexDescription &index : all_indexes)
    {
        if (index.type != "text")
            continue;

        chassert(index.column_names.size() == 1);
        map_column_index.emplace(index.column_names.front(), index.name);
        printf(" Index: %s %s\n", index.column_names.front().c_str(), index.name.c_str());
        // if [!inserted] TODO: JAM decide if we need to handle the case where a column repeats (two indexes exist for the same column)
    }

    // for (const auto &col : metadata->getColumns())
    // {
    //     printf(" Column: %s %s %d\n",
    //         col.name.c_str(), col.type->getName().c_str(), col.type->getTypeId() == TypeIndex::String);
    // }

    // printf("Indices: %s\n", all_indexes.toString().c_str());
    // // TODO: JAM At this point it may be possible to set few optimizations
    // printf(" Indices Column: %s\n", index.column_names.front().c_str());
    // printf(" Indices Type: %s\n", index.type.c_str());
    // printf(" Indices Name: %s\n", index.name.c_str());

    return 0;
}

}
