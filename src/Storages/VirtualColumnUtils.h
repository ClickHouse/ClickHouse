#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnsDescription.h>

#include <unordered_set>


namespace DB
{

class NamesAndTypesList;


namespace VirtualColumnUtils
{

/// The filtering functions are tricky to use correctly.
/// There are 2 ways:
///  1. Call filterBlockWithPredicate() or filterBlockWithExpression() inside SourceStepWithFilter::applyFilters().
///  2. Call splitFilterDagForAllowedInputs() and buildSetsForDAG() inside SourceStepWithFilter::applyFilters().
///     Then call filterBlockWithPredicate() or filterBlockWithExpression() in initializePipeline().
///
/// Otherwise calling filter*() outside applyFilters() will throw "Not-ready Set is passed"
/// if there are subqueries.

/// Similar to filterBlockWithExpression(buildFilterExpression(splitFilterDagForAllowedInputs(...))).
void filterBlockWithPredicate(const ActionsDAG::Node * predicate, Block & block, ContextPtr context);

/// Just filters block. Block should contain all the required columns.
ExpressionActionsPtr buildFilterExpression(ActionsDAG dag, ContextPtr context);
void filterBlockWithExpression(const ExpressionActionsPtr & actions, Block & block);

/// Builds sets used by ActionsDAG inplace.
void buildSetsForDAG(const ActionsDAG & dag, const ContextPtr & context);

/// Recursively checks if all functions used in DAG are deterministic in scope of query.
bool isDeterministicInScopeOfQuery(const ActionsDAG::Node * node);

/// Extract a part of predicate that can be evaluated using only columns from input_names.
std::optional<ActionsDAG> splitFilterDagForAllowedInputs(const ActionsDAG::Node * predicate, const Block * allowed_inputs);

/// Extract from the input stream a set of `name` column values
template <typename T>
auto extractSingleValueFromBlock(const Block & block, const String & name)
{
    std::unordered_set<T> res;
    const ColumnWithTypeAndName & data = block.getByName(name);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        res.insert((*data.column)[i].get<T>());
    return res;
}

NameSet getVirtualNamesForFileLikeStorage();
VirtualColumnsDescription getVirtualsForFileLikeStorage(const ColumnsDescription & storage_columns);

std::optional<ActionsDAG> createPathAndFileFilterDAG(const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns);

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns);

template <typename T>
void filterByPathOrFile(std::vector<T> & sources, const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns)
{
    auto indexes_column = getFilterByPathAndFileIndexes(paths, actions, virtual_columns);
    const auto & indexes = typeid_cast<const ColumnUInt64 &>(*indexes_column).getData();
    if (indexes.size() == sources.size())
        return;

    std::vector<T> filtered_sources;
    filtered_sources.reserve(indexes.size());
    for (auto index : indexes)
        filtered_sources.emplace_back(std::move(sources[index]));
    sources = std::move(filtered_sources);
}

struct VirtualsForFileLikeStorage
{
    const String & path;
    std::optional<size_t> size { std::nullopt };
    const String * filename { nullptr };
    std::optional<Poco::Timestamp> last_modified { std::nullopt };
    const String * etag { nullptr };
};

void addRequestedFileLikeStorageVirtualsToChunk(
    Chunk & chunk, const NamesAndTypesList & requested_virtual_columns,
    VirtualsForFileLikeStorage virtual_values);
}

}
