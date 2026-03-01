#pragma once

#include <functional>
#include <unordered_map>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB
{

struct RangesInDataParts;
struct StorageID;
class ActionsDAG;

/// <part_name, ranges>
using IndexAnalysisPartsRanges = std::unordered_map<std::string, MarkRanges>;
/// <replica index, <replica address, parts ranges>>
using DistributedIndexAnalysisPartsRanges = std::vector<std::pair<std::string, IndexAnalysisPartsRanges>>;

using LocalIndexAnalysisCallback = std::function<IndexAnalysisPartsRanges(const std::vector<std::string_view> & parts)>;

/// Do index analysis on replicas from the cluster_for_parallel_replicas
/// by sending mergeTreeAnalyzeIndexesUUID() to each replica with list of assigned parts,
/// in case of any failures the analysis will be done on local replica.
///
/// For local replica uses LocalIndexAnalysisCallback (can be called multiple times).
DistributedIndexAnalysisPartsRanges distributedIndexAnalysisOnReplicas(
    const StorageID & storage_id,
    const ActionsDAG * filter_actions_dag,
    const NameSet & indexes_column_names,
    const RangesInDataParts & parts_with_ranges,
    const OptionalVectorSearchParameters & vector_search_parameters,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context);

}
