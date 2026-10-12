#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/BM25State.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>

#include <algorithm>
#include <unordered_set>

namespace ProfileEvents
{
    extern const Event TextScoreStatsBuilt;
    extern const Event TextScoreStatsBuildMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectExecutorThreads;
    extern const Metric MergeTreeDataSelectExecutorThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_for_indexes;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

BM25GlobalStatsBuilder::BM25GlobalStatsBuilder(const IndexReadTask & index_read_task)
    : index_with_condition(index_read_task.index)
    , params(index_read_task.bm25_params.value())
    , scoring_token_names(index_read_task.scoring_tokens.begin(), index_read_task.scoring_tokens.end())
{
    text_index = &typeid_cast<const MergeTreeIndexText &>(*index_with_condition.index.get());
    condition_text = &typeid_cast<const MergeTreeIndexConditionText &>(*index_with_condition.condition_template->generateUnsubstituted());
    std::ranges::sort(scoring_token_names);

    if (scoring_token_names.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot compute text score: the read task of text index '{}' has no scoring tokens",
            text_index->index.name);
    }

    document_frequencies = std::vector<std::atomic<UInt64>>(scoring_token_names.size());
    ProfileEvents::increment(ProfileEvents::TextScoreStatsBuilt);
}

void BM25GlobalStatsBuilder::addPart(const DataPartPtr & part, const MergeTreeReaderSettings & reader_settings)
{
    if (part->isEmpty())
        return;

    if (!text_index->getDeserializedFormat(*part, text_index->getFileName()))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot compute text score: the text index '{}' is not materialized in part '{}'. "
            "Run 'ALTER TABLE ... MATERIALIZE INDEX {}' first",
            text_index->index.name, part->name, text_index->index.name);
    }

    /// The document frequencies do not depend on the filter, so the tokens are looked up regardless of the search queries.
    LoadedMergeTreeDataPartInfoForReader part_info(part, std::make_shared<AlterConversions>());
    auto tokens_lookup = lookupTextIndexTokens(part_info, *text_index, *condition_text, scoring_token_names, reader_settings);

    const auto & scoring_stats = tokens_lookup.header->scoring_stats;
    if (tokens_lookup.header->scoring != TextIndexScoringKind::BM25)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot compute text score: the text index '{}' in part '{}' was written without BM25 scoring data. "
            "Recreate the index with `scoring = 'bm25'` and run 'ALTER TABLE ... MATERIALIZE INDEX {}'",
            text_index->index.name, part->name, text_index->index.name);
    }

    num_docs.fetch_add(scoring_stats.num_docs, std::memory_order_relaxed);
    sum_doc_length.fetch_add(scoring_stats.sum_doc_length, std::memory_order_relaxed);

    for (size_t i = 0; i < scoring_token_names.size(); ++i)
    {
        auto it = tokens_lookup.token_infos.find(scoring_token_names[i]);

        if (it != tokens_lookup.token_infos.end())
            document_frequencies[i].fetch_add(it->second->cardinality, std::memory_order_relaxed);
    }
}

BM25StatePtr BM25GlobalStatsBuilder::build() const
{
    const UInt64 total_docs = num_docs.load(std::memory_order_relaxed);
    const UInt64 total_doc_length = sum_doc_length.load(std::memory_order_relaxed);
    const Float64 avg_doc_length = total_docs ? static_cast<Float64>(total_doc_length) / static_cast<Float64>(total_docs) : 0.0;

    auto state = std::make_shared<BM25State>();
    state->length_norm_cache = std::make_shared<const BM25LengthNormCache>(avg_doc_length, params);
    state->tokens.reserve(scoring_token_names.size());

    for (size_t i = 0; i < scoring_token_names.size(); ++i)
    {
        auto idf = calculateIDF(total_docs, document_frequencies[i].load(std::memory_order_relaxed));
        BM25Weight weight(idf, params, state->length_norm_cache.get());
        state->tokens.push_back(BM25ScoringToken{.token = scoring_token_names[i], .weight = weight});
    }

    return state;
}

BM25StatePtr buildBM25State(
    const RangesInDataParts & parts_ranges,
    const IndexReadTasks & index_read_tasks,
    const MergeTreeReaderSettings & reader_settings,
    const ContextPtr & context)
{
    const IndexReadTask * score_task = nullptr;

    for (const auto & [_, index_task] : index_read_tasks)
    {
        if (index_task.bm25_params)
        {
            score_task = &index_task;
            break;
        }
    }

    if (!score_task)
        return nullptr;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextScoreStatsBuildMicroseconds);
    auto builder = std::make_shared<BM25GlobalStatsBuilder>(*score_task);

    /// A part can appear in several entries, its statistics must be accumulated once.
    std::unordered_set<DataPartPtr> parts;
    for (const auto & part_with_ranges : parts_ranges)
    {
        if (part_with_ranges.data_part)
            parts.insert(part_with_ranges.data_part);
    }

    const auto & settings = context->getSettingsRef();
    size_t num_threads = std::min<size_t>(parts.size(), settings[Setting::max_threads]);

    if (settings[Setting::max_threads_for_indexes])
    {
        num_threads = std::min<size_t>(num_threads, settings[Setting::max_threads_for_indexes]);
    }

    if (num_threads <= 1)
    {
        for (const auto & part : parts)
            builder->addPart(part, reader_settings);
    }
    else
    {
        /// Borrow threads from the global pool with a timeout to avoid a deadlock when it is saturated.
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectExecutorThreads,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (const auto & part : parts)
        {
            pool.scheduleOrThrow(
                [&, part, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                    builder->addPart(part, reader_settings);
                },
                Priority{},
                settings[Setting::lock_acquire_timeout].totalMicroseconds());
        }

        pool.wait();
    }

    return builder->build();
}

}
