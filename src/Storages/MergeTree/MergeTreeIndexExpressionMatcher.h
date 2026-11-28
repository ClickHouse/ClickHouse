#pragma once

#include <Core/Block_fwd.h>
#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>

#include <optional>

namespace DB
{

class RPNBuilderTreeNode;

/// Whether `query_node` computes the same value as the index expression node `index_node`, compared by structure.
/// `normalize_empty_string_comparisons`: the query's `empty(a)` / `notEmpty(a)` also match `a = ''` / `a != ''`.
bool isSameIndexExpression(
    const ActionsDAG::Node * query_node, const ActionsDAG::Node * index_node, bool normalize_empty_string_comparisons = false);

/// `index_nodes` are the index expression outputs, by position in `header`.
std::optional<size_t> findIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const RPNBuilderTreeNode & node,
    bool normalize_empty_string_comparisons = false);

/// Position of the index column `function_name(<argument>)` where the index applies `function_name` itself to
/// `argument_node`, or to the table column `argument_name` when `argument_node` is nullptr.
std::optional<size_t> findFunctionIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const String & function_name,
    const String & argument_name,
    const ActionsDAG::Node * argument_node,
    bool normalize_empty_string_comparisons = false);

/// The same for `mapKeys` / `mapValues` of a `Map`.
std::optional<size_t> findMapIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const String & function_name,
    const String & map_column_name,
    const ActionsDAG::Node * map_node,
    bool normalize_empty_string_comparisons = false);

/// Names of `header`, with all but `JSONAllPaths` / `JSONAllValues` of a table column left empty.
Names collectJSONIndexColumns(const Block & header, const ActionsDAG::NodeRawConstPtrs & index_nodes);

}
