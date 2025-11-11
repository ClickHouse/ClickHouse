#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace CurrentMetrics
{
    extern const Metric FilteringMarksWithSecondaryKeys;
}

namespace ProfileEvents
{
    extern const Event FilteringMarksWithSecondaryKeysMicroseconds;
}

namespace DB
{

MergeTreeSkipIndexReader::MergeTreeSkipIndexReader(
    UsefulSkipIndexes skip_indexes_,
    MarkCachePtr mark_cache_,
    UncompressedCachePtr uncompressed_cache_,
    VectorSimilarityIndexCachePtr vector_similarity_index_cache_,
    MergeTreeReaderSettings reader_settings_,
    LoggerPtr log_)
    : skip_indexes(std::move(skip_indexes_))
    , mark_cache(std::move(mark_cache_))
    , uncompressed_cache(std::move(uncompressed_cache_))
    , vector_similarity_index_cache(std::move(vector_similarity_index_cache_))
    , reader_settings(std::move(reader_settings_))
    , log(std::move(log_))
{
}

SkipIndexReadResultPtr MergeTreeSkipIndexReader::read(const RangesInDataPart & part)
{
    CurrentMetrics::Increment metric(CurrentMetrics::FilteringMarksWithSecondaryKeys);

    auto ranges = part.ranges;
    size_t ending_mark = ranges.empty() ? 0 : ranges.back().end;
    for (const auto & index_and_condition : skip_indexes.useful_indices)
    {
        if (is_cancelled)
            return {};

        if (ranges.empty())
            break;

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

        ranges = MergeTreeDataSelectExecutor::filterMarksUsingIndex(
            index_and_condition.index,
            index_and_condition.condition,
            part.data_part,
            ranges,
            part.read_hints,
            reader_settings,
            mark_cache.get(),
            uncompressed_cache.get(),
            vector_similarity_index_cache.get(),
            log).first;
    }

    for (const auto & indices_and_condition : skip_indexes.merged_indices)
    {
        if (is_cancelled)
            return {};

        if (ranges.empty())
            break;

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

        ranges = MergeTreeDataSelectExecutor::filterMarksUsingMergedIndex(
            indices_and_condition.indices,
            indices_and_condition.condition,
            part.data_part,
            ranges,
            reader_settings,
            mark_cache.get(),
            uncompressed_cache.get(),
            vector_similarity_index_cache.get(),
            log);
    }

    if (is_cancelled)
        return {};

    auto res = std::make_shared<SkipIndexReadResult>(ending_mark);
    for (const auto & range : ranges)
    {
        for (auto i = range.begin; i < range.end; ++i)
            (*res)[i] = true;
    }
    return res;
}

MergeTreeIndexReadResultPool::MergeTreeIndexReadResultPool(MergeTreeSkipIndexReaderPtr skip_index_reader_)
    : skip_index_reader(std::move(skip_index_reader_))
{
    chassert(skip_index_reader);
}

MergeTreeIndexReadResultPtr MergeTreeIndexReadResultPool::getOrBuildIndexReadResult(const RangesInDataPart & part)
{
    std::unique_lock lock(index_read_result_registry_mutex);
    auto it = index_read_result_registry.find(part.data_part.get());

    if (it == index_read_result_registry.end())
    {
        auto promise = index_read_result_registry.emplace(part.data_part.get(), IndexReadResultEntry{}).first->second.promise;
        lock.unlock();
        try
        {
            MergeTreeIndexReadResultPtr res;
            auto skip_index_res = skip_index_reader->read(part);
            if (skip_index_res)
            {
                res = std::make_shared<MergeTreeIndexReadResult>();
                res->skip_index_read_result = std::move(skip_index_res);
            }

            /// TODO(ab): If projection index is available, attempt to build projection index read result.

            promise->set_value(res);
            return res;
        }
        catch (...)
        {
            promise->set_value(nullptr);
            throw;
        }
    }
    else
    {
        auto future = it->second.future;
        lock.unlock();
        return future.get();
    }
}

void MergeTreeIndexReadResultPool::clear(const DataPartPtr & part)
{
    std::lock_guard lock(index_read_result_registry_mutex);
    index_read_result_registry.erase(part.get());
}

void MergeTreeIndexReadResultPool::cancel() noexcept
{
    skip_index_reader->cancel();

    /// TODO(ab): If projection index is available, cancel projection index reader here.
}

}
