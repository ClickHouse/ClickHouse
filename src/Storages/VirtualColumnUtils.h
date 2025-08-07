#pragma once

#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Formats/FormatSettings.h>
#include <absl/container/flat_hash_map.h>


namespace DB
{

class Block;
class Chunk;
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
///
/// Similar to filterBlockWithExpression(buildFilterExpression(splitFilterDagForAllowedInputs(...)))./// Similar to filterBlockWithQuery, but uses ActionsDAG as a predicate.
/// Basically it is filterBlockWithDAG(splitFilterDagForAllowedInputs).
/// If allow_filtering_with_partial_predicate is true, then the filtering will be done even if some part of the predicate
/// cannot be evaluated using the columns from the block.
void filterBlockWithPredicate(const ActionsDAG::Node * predicate, Block & block, ContextPtr context, bool allow_filtering_with_partial_predicate = true);


/// Just filters block. Block should contain all the required columns.
ExpressionActionsPtr buildFilterExpression(ActionsDAG dag, ContextPtr context);
void filterBlockWithExpression(const ExpressionActionsPtr & actions, Block & block);

/// Builds sets used by ActionsDAG inplace.
void buildSetsForDAG(const ActionsDAG & dag, const ContextPtr & context);

/// Checks if all functions used in DAG are deterministic.
bool isDeterministic(const ActionsDAG::Node * node);

/// Checks recursively if all functions used in DAG are deterministic in scope of query.
bool isDeterministicInScopeOfQuery(const ActionsDAG::Node * node);

/// Extract a part of predicate that can be evaluated using only columns from input_names.
/// When allow_partial_result is false, then the result will be empty if any part of if cannot be evaluated deterministically
/// on the given inputs.
/// allow_partial_result must be false when we are going to use the result to filter parts in
/// MergeTreeData::totalRowsByPartitionPredicateImp. For example, if the query is
/// `SELECT count() FROM table  WHERE _partition_id = '0' AND rowNumberInBlock() = 1`
/// The predicate will be `_partition_id = '0' AND rowNumberInBlock() = 1`, and `rowNumberInBlock()` is
/// non-deterministic. If we still extract the part `_partition_id = '0'` for filtering parts, then trivial
/// count optimization will be mistakenly applied to the query.
std::optional<ActionsDAG> splitFilterDagForAllowedInputs(const ActionsDAG::Node * predicate, const Block * allowed_inputs, bool allow_partial_result = true);

/// Extract from the input stream a set of `name` column values
template <typename T>
auto extractSingleValueFromBlock(const Block & block, const String & name)
{
    std::unordered_set<T> res;
    const ColumnWithTypeAndName & data = block.getByName(name);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        res.insert((*data.column)[i].safeGet<T>());
    return res;
}

NameSet getVirtualNamesForFileLikeStorage();
VirtualColumnsDescription getVirtualsForFileLikeStorage(
    ColumnsDescription & storage_columns,
    const ContextPtr & context,
    const std::string & sample_path = "",
    std::optional<FormatSettings> format_settings_ = std::nullopt,
    bool is_data_lake = false);

std::optional<ActionsDAG> createPathAndFileFilterDAG(const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns);

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns, const ContextPtr & context);

template <typename T>
void filterByPathOrFile(std::vector<T> & sources, const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
{
    auto indexes_column = getFilterByPathAndFileIndexes(paths, actions, virtual_columns, context);
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
    VirtualsForFileLikeStorage virtual_values, ContextPtr context);

using HivePartitioningKeysAndValues = absl::flat_hash_map<std::string_view, std::string_view>;

HivePartitioningKeysAndValues parseHivePartitioningKeysAndValues(const String & path);

}

}
