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

/// Analysis result of one replica, with the number of parts and marks assigned to it for
/// analysis (a part split into segments contributes only its assigned segments per replica,
/// and counts once per replica it is assigned to).
struct DistributedIndexAnalysisReplicaResult
{
    std::string address;
    size_t assigned_parts = 0;
    size_t assigned_marks = 0;
    IndexAnalysisPartsRanges parts_ranges;
};
/// Indexed by replica.
using DistributedIndexAnalysisPartsRanges = std::vector<DistributedIndexAnalysisReplicaResult>;

/// Parts assigned to a replica, each with the mark ranges to analyze (empty => the whole part).
using AssignedPartsRanges = std::vector<std::pair<std::string_view, MarkRanges>>;
using LocalIndexAnalysisCallback = std::function<IndexAnalysisPartsRanges(const AssignedPartsRanges & parts)>;

/// Do index analysis on replicas from the cluster_for_parallel_replicas
/// by sending mergeTreeAnalyzeIndexesUUID() to each replica with list of assigned parts,
/// in case of any failures the analysis will be done on local replica.
///
/// For local replica uses LocalIndexAnalysisCallback (can be called multiple times).
DistributedIndexAnalysisPartsRanges distributedIndexAnalysisOnReplicas(
    const StorageID & storage_id,
    const ActionsDAG * filter_actions_dag,
    ASTPtr sampling_filter,
    const NameSet & indexes_column_names,
    const RangesInDataParts & parts_with_ranges,
    const OptionalVectorSearchParameters & vector_search_parameters,
    size_t mark_segment_size,
    size_t min_marks_to_split_part,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context);

}
