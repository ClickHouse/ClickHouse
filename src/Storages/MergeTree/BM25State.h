#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/BM25Kernel.h>

#include <atomic>
#include <map>
#include <memory>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

class MergeTreeIndexText;
class MergeTreeIndexConditionText;

/// Query-global BM25 state shared by all read tasks and threads of one query.
struct BM25State
{
    std::shared_ptr<const BM25LengthNormCache> length_norm_cache;
    std::vector<BM25ScoringToken> tokens;
};

using BM25StatePtr = std::shared_ptr<const BM25State>;

/// Thread-safe accumulator for the query-global BM25 collection statistics.
class BM25GlobalStatsBuilder
{
public:
    explicit BM25GlobalStatsBuilder(MergeTreeIndexWithCondition index_with_condition_);

    void addPart(const DataPartPtr & part, const MergeTreeReaderSettings & reader_settings);
    BM25StatePtr build() const;

private:
    MergeTreeIndexWithCondition index_with_condition;
    const MergeTreeIndexText * text_index;
    const MergeTreeIndexConditionText * condition_text;
    std::vector<String> scoring_token_names;

    std::atomic<UInt64> num_docs{0};
    std::atomic<UInt64> sum_doc_length{0};
    std::vector<std::atomic<UInt64>> document_frequencies;
};

struct RangesInDataParts;
struct MergeTreeReaderSettings;
struct IndexReadTask;
using IndexReadTasks = std::map<String, IndexReadTask>;

/// Builds the query-global BM25 state (IDF, average document length) for calculating the BM25 score.
/// Runs one parallel pass over the parts' text-index granules.
/// Returns null when no index read task carries the score column.
BM25StatePtr buildBM25State(
    const RangesInDataParts & parts_ranges,
    const IndexReadTasks & index_read_tasks,
    const MergeTreeReaderSettings & reader_settings,
    const ContextPtr & context);

}
