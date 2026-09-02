#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/BM25State.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>

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

namespace
{

/// Opens the part's base text-index substreams and deserializes its granule
std::shared_ptr<const MergeTreeIndexGranuleText> loadTextIndexGranuleForStats(
    const DataPartPtr & part,
    const MergeTreeIndexText & text_index,
    const MergeTreeIndexConditionText & condition_text,
    const MergeTreeIndexFormat & index_format,
    const MergeTreeReaderSettings & reader_settings)
{
    auto substreams = text_index.getSubstreams();
    auto data_part_storage = part->getDataPartStoragePtr();

    auto make_stream = [&](const MergeTreeIndexSubstream & substream)
    {
        return makeTextIndexInputStream(
            data_part_storage,
            text_index.getFileName() + substream.suffix,
            substream.extension,
            MergeTreeIndexReader::patchSettings(reader_settings, substream.type));
    };

    auto sparse_index_stream = make_stream(substreams[0]);
    auto dictionary_stream = make_stream(substreams[1]);
    auto postings_stream = make_stream(substreams[2]);

    sparse_index_stream->seekToStart();

    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexDictionary] = dictionary_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexPostings] = postings_stream.get();

    LoadedMergeTreeDataPartInfoForReader part_info(part, std::make_shared<AlterConversions>());

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = &condition_text,
        .part_info = part_info,
        .index = text_index,
        .readable_ranges = nullptr,
        .text_index_read_postings = false,
    };

    auto granule = text_index.createIndexGranule();
    granule->deserializeBinaryWithMultipleStreams(streams, state);
    return std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(std::move(granule));
}

}

BM25GlobalStatsBuilder::BM25GlobalStatsBuilder(MergeTreeIndexWithCondition index_with_condition_)
    : index_with_condition(std::move(index_with_condition_))
{
    text_index = &typeid_cast<const MergeTreeIndexText &>(*index_with_condition.index.get());
    condition_text = &typeid_cast<const MergeTreeIndexConditionText &>(*index_with_condition.condition_template->generateUnsubstituted());
    scoring_token_names = condition_text->getScoringTokens();

    if (!condition_text->isScoringEnabled() || scoring_token_names.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot compute text score: the condition of text index '{}' has no scoring tokens",
            text_index->index.name);
    }

    document_frequencies = std::vector<std::atomic<UInt64>>(scoring_token_names.size());
    ProfileEvents::increment(ProfileEvents::TextScoreStatsBuilt);
}

void BM25GlobalStatsBuilder::addPart(const DataPartPtr & part, const MergeTreeReaderSettings & reader_settings)
{
    if (part->isEmpty())
        return;

    auto index_format = text_index->getDeserializedFormat(*part, text_index->getFileName());
    if (!index_format)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot compute text score: the text index '{}' is not materialized in part '{}'. "
            "Run 'ALTER TABLE ... MATERIALIZE INDEX {}' first",
            text_index->index.name, part->name, text_index->index.name);
    }

    auto granule = loadTextIndexGranuleForStats(part, *text_index, *condition_text, index_format, reader_settings);
    if (!granule)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index '{}' produced a granule of an unexpected type", text_index->index.name);
    }

    const auto & scoring_stats = granule->getScoringStats();
    if (granule->getSerializationVersion() < MergeTreeTextIndexSerializationVersion::V3_WithScoring || !scoring_stats.hasSegmentedDocLengths())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot compute text score: the text index '{}' in part '{}' was written without scoring data. "
            "Recreate the index with 'enable_scoring = 1' and run 'ALTER TABLE ... MATERIALIZE INDEX {}'",
            text_index->index.name, part->name, text_index->index.name);
    }

    num_docs.fetch_add(scoring_stats.num_docs, std::memory_order_relaxed);
    sum_doc_length.fetch_add(scoring_stats.sum_doc_length, std::memory_order_relaxed);
    const auto & token_infos = granule->getAnalyzer().getAllTokenInfos();

    for (size_t i = 0; i < scoring_token_names.size(); ++i)
    {
        auto it = token_infos.find(scoring_token_names[i]);

        if (it != token_infos.end() && it->second)
            document_frequencies[i].fetch_add(it->second->cardinality, std::memory_order_relaxed);
    }
}

BM25StatePtr BM25GlobalStatsBuilder::build() const
{
    const BM25Params params;
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
        if (index_task.columns.contains(BM25ScoreColumn::name))
        {
            score_task = &index_task;
            break;
        }
    }

    if (!score_task)
        return nullptr;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextScoreStatsBuildMicroseconds);
    auto builder = std::make_shared<BM25GlobalStatsBuilder>(score_task->index);

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
