#pragma once

#include <functional>
#include <unordered_map>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

struct RangesInDataParts;
struct StorageID;
struct SelectQueryInfo;

/// <part_name, ranges>
using IndexAnalysisPartsRanges = std::unordered_map<std::string, MarkRanges>;
/// <replica index, <replica address, parts ranges>>
using DistributedIndexAnalysisPartsRanges = std::vector<std::pair<std::string, IndexAnalysisPartsRanges>>;

using LocalIndexAnalysisCallback = std::function<IndexAnalysisPartsRanges(const std::vector<std::string_view> & parts)>;

/// Do index analysis on replicas from the cluster_for_parallel_replicas
/// by sending mergeTreeAnalyzeIndexUUID() to each replica with list of assigned parts,
/// in case of any failures the analysis will be done on local replica.
///
/// For local replica uses LocalIndexAnalysisCallback (can be called multiple times).
DistributedIndexAnalysisPartsRanges distributedIndexAnalysisOnReplicas(
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const RangesInDataParts & parts_with_ranges,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context);

}
