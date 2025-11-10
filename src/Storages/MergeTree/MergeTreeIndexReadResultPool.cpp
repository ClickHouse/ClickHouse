#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadPoolProjectionIndex.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

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

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

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

ProjectionIndexBitmap::ProjectionIndexBitmap(BitmapType bitmap_type)
    : type(bitmap_type)
{
    if (type == BitmapType::Bitmap32)
        data.bitmap32 = roaring::api::roaring_bitmap_create();
    else
        data.bitmap64 = roaring::api::roaring64_bitmap_create();
}

ProjectionIndexBitmap::~ProjectionIndexBitmap()
{
    if (type == BitmapType::Bitmap32)
    {
        if (data.bitmap32)
            roaring_bitmap_free(data.bitmap32);
    }
    else
    {
        if (data.bitmap64)
            roaring64_bitmap_free(data.bitmap64);
    }
}

ProjectionIndexBitmapPtr ProjectionIndexBitmap::create32()
{
    return std::make_shared<ProjectionIndexBitmap>(BitmapType::Bitmap32);
}

ProjectionIndexBitmapPtr ProjectionIndexBitmap::create64()
{
    return std::make_shared<ProjectionIndexBitmap>(BitmapType::Bitmap64);
}

void ProjectionIndexBitmap::intersectWith(const ProjectionIndexBitmap & other) // NOLINT
{
    chassert(type == other.type);
    if (type == BitmapType::Bitmap32)
        roaring_bitmap_and_inplace(data.bitmap32, other.data.bitmap32);
    else
        roaring64_bitmap_and_inplace(data.bitmap64, other.data.bitmap64);
}

size_t ProjectionIndexBitmap::cardinality() const
{
    if (type == BitmapType::Bitmap32)
        return roaring_bitmap_get_cardinality(data.bitmap32);
    else
        return roaring64_bitmap_get_cardinality(data.bitmap64);
}

bool ProjectionIndexBitmap::empty() const
{
    if (type == BitmapType::Bitmap32)
        return roaring_bitmap_is_empty(data.bitmap32);
    else
        return roaring64_bitmap_is_empty(data.bitmap64);
}

template <typename Offset>
bool ProjectionIndexBitmap::contains(std::type_identity_t<Offset> value)
{
    static_assert(
        std::is_same_v<Offset, UInt32> || std::is_same_v<Offset, UInt64>,
        "ProjectionIndexBitmap::contains<> supports only UInt32 or UInt64");

    if constexpr (std::is_same_v<Offset, UInt32>)
    {
        chassert(type == BitmapType::Bitmap32);
        return roaring_bitmap_contains(data.bitmap32, value);
    }
    else
    {
        chassert(type == BitmapType::Bitmap64);
        return roaring64_bitmap_contains(data.bitmap64, value);
    }
}

template <typename Offset>
void ProjectionIndexBitmap::add(std::type_identity_t<Offset> value)
{
    static_assert(
        std::is_same_v<Offset, UInt32> || std::is_same_v<Offset, UInt64>,
        "ProjectionIndexBitmap::add<> supports only UInt32 or UInt64");

    if constexpr (std::is_same_v<Offset, UInt32>)
    {
        chassert(type == BitmapType::Bitmap32);
        roaring_bitmap_add(data.bitmap32, value);
    }
    else
    {
        chassert(type == BitmapType::Bitmap64);
        roaring64_bitmap_add(data.bitmap64, value);
    }
}

template <typename Offset>
void ProjectionIndexBitmap::addBulk(const std::type_identity_t<Offset> * values, size_t size)
{
    static_assert(
        std::is_same_v<Offset, UInt32> || std::is_same_v<Offset, UInt64>,
        "ProjectionIndexBitmap::addBulk<> supports only UInt32 or UInt64");

    if constexpr (std::is_same_v<Offset, UInt32>)
    {
        chassert(type == BitmapType::Bitmap32);
        roaring::api::roaring_bulk_context_t context{};
        for (size_t i = 0; i < size; ++i)
            roaring_bitmap_add_bulk(data.bitmap32, &context, values[i]);
    }
    else
    {
        chassert(type == BitmapType::Bitmap64);
        roaring::api::roaring64_bulk_context_t context{};
        for (size_t i = 0; i < size; ++i)
            roaring64_bitmap_add_bulk(data.bitmap64, &context, values[i]);
    }
}

bool ProjectionIndexBitmap::rangeAllZero(size_t begin, size_t end) const
{
    if (type == BitmapType::Bitmap32)
    {
        roaring::api::roaring_uint32_iterator_t it;
        roaring_iterator_init(data.bitmap32, &it);
        if (!roaring_uint32_iterator_move_equalorlarger(&it, begin))
            return true;

        return it.current_value >= end;
    }
    else
    {
        chassert(type == BitmapType::Bitmap64);
        /// NOTE: roaring64 requires a heap-allocated opaque iterator (unlike 32-bit)
        auto * it = roaring::api::roaring64_iterator_create(data.bitmap64);
        /// There is no way to recover a failed allocation inside roaring64.
        /// See https://github.com/RoaringBitmap/CRoaring/issues/638
        if (it == nullptr)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failed to allocate roaring64 iterator");

        if (!roaring::api::roaring64_iterator_move_equalorlarger(it, begin))
        {
            roaring::api::roaring64_iterator_free(it);
            return true;
        }

        auto val = roaring64_iterator_value(it);
        roaring::api::roaring64_iterator_free(it);
        return val >= end;
    }
}


bool ProjectionIndexBitmap::appendToFilter(PaddedPODArray<UInt8> & filter, size_t starting_row, size_t num_rows) const
{
    size_t old_size = filter.size();
    filter.resize_fill(old_size + num_rows);
    UInt8 * pos = &filter[old_size];
    size_t ending_row = starting_row + num_rows;

    if (type == BitmapType::Bitmap32)
    {
        roaring::api::roaring_uint32_iterator_t it;
        roaring_iterator_init(data.bitmap32, &it);
        if (!roaring_uint32_iterator_move_equalorlarger(&it, starting_row))
            return false;

        bool has_value = false;
        while (it.current_value < ending_row)
        {
            has_value = true;
            pos[it.current_value - starting_row] = 1;
            if (!roaring_uint32_iterator_advance(&it))
                break;
        }
        return has_value;
    }
    else
    {
        chassert(type == BitmapType::Bitmap64);
        /// NOTE: roaring64 requires a heap-allocated opaque iterator (unlike 32-bit)
        auto * it = roaring::api::roaring64_iterator_create(data.bitmap64);
        /// There is no way to recover a failed allocation inside roaring64.
        /// See https://github.com/RoaringBitmap/CRoaring/issues/638
        if (it == nullptr)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failed to allocate roaring64 iterator");

        if (!roaring::api::roaring64_iterator_move_equalorlarger(it, starting_row))
        {
            roaring::api::roaring64_iterator_free(it);
            return false;
        }

        auto val = roaring64_iterator_value(it);
        bool has_value = false;
        while (val < ending_row)
        {
            has_value = true;
            pos[val - starting_row] = 1;
            if (!roaring::api::roaring64_iterator_advance(it))
                break;
            val = roaring64_iterator_value(it);
        }
        roaring::api::roaring64_iterator_free(it);
        return has_value;
    }
}

template bool ProjectionIndexBitmap::contains<UInt32>(UInt32 value);
template bool ProjectionIndexBitmap::contains<UInt64>(UInt64 value);

template void ProjectionIndexBitmap::add<UInt32>(UInt32 value);
template void ProjectionIndexBitmap::add<UInt64>(UInt64 value);

template void ProjectionIndexBitmap::addBulk<UInt32>(const UInt32 * values, size_t size);
template void ProjectionIndexBitmap::addBulk<UInt64>(const UInt64 * values, size_t size);

SingleProjectionIndexReader::SingleProjectionIndexReader(
    std::shared_ptr<MergeTreeReadPoolProjectionIndex> pool,
    PrewhereInfoPtr prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings)
    : projection_index_read_pool(std::move(pool))
    , processor(std::make_unique<MergeTreeSelectProcessor>(
          std::static_pointer_cast<IMergeTreeReadPool>(projection_index_read_pool),
          std::make_unique<MergeTreeProjectionIndexSelectAlgorithm>(),
          nullptr /*row_level_filter*/,
          std::move(prewhere_info),
          nullptr /*lazily_read_info*/,
          IndexReadTasks{} /*index_read_tasks*/,
          actions_settings,
          reader_settings))
{
}

ProjectionIndexBitmapPtr SingleProjectionIndexReader::read(const RangesInDataPart & ranges)
{
    bool can_use_32bit_part_offset = ranges.parent_ranges.max_part_offset <= std::numeric_limits<UInt32>::max();

    /// Prepare the read processor with the current part and its read ranges.
    /// This sets up internal state needed to read the projection data.
    assert_cast<MergeTreeProjectionIndexSelectAlgorithm &>(*processor->algorithm).preparePartToRead(&ranges);
    auto res = can_use_32bit_part_offset ? ProjectionIndexBitmap::create32() : ProjectionIndexBitmap::create64();

    /// Start reading chunks from the projection index reader.
    /// Each chunk contains a column of UInt64 offsets that we insert into the bitmap.
    while (true)
    {
        auto chunk = processor->read();
        if (chunk.chunk)
        {
            if (chunk.chunk.getNumRows() > 0)
            {
                chassert(chunk.chunk.getColumns().size() == 1);
                auto offset_column = chunk.chunk.getColumns()[0]->convertToFullIfNeeded();
                const auto & offsets = assert_cast<const ColumnUInt64 &>(*offset_column);

                auto add_offsets = [&]<typename Offset>(Offset)
                {
                    if (ranges.parent_ranges.isContiguousFullRange())
                    {
                        for (auto offset : offsets.getData())
                            res->add<Offset>(offset);
                    }
                    else
                    {
                        for (auto offset : offsets.getData())
                        {
                            if (ranges.parent_ranges.contains(offset))
                                res->add<Offset>(offset);
                        }
                    }
                };
                if (can_use_32bit_part_offset)
                    add_offsets(UInt32{});
                else
                    add_offsets(UInt64{});
            }
        }

        if (chunk.is_finished)
            break;
    }

    /// If the read was cancelled, return nullptr to avoid using an incomplete index bitmap.
    if (processor->is_cancelled)
        res = nullptr;

    return res;
}

void SingleProjectionIndexReader::cancel() noexcept
{
    processor->cancel();
}

MergeTreeProjectionIndexReader::MergeTreeProjectionIndexReader(ProjectionIndexReaderByName projection_index_readers_)
    : projection_index_readers(std::move(projection_index_readers_))
{
}

ProjectionIndexBitmapPtr MergeTreeProjectionIndexReader::read(const RangesInDataParts & projection_parts)
{
    ProjectionIndexBitmaps bitmaps;
    for (const auto & ranges : projection_parts)
    {
        const auto & proj_name = ranges.data_part->name;
        auto & reader = projection_index_readers.at(proj_name);
        auto res = reader.read(ranges);

        /// If any bitmap is incomplete (due to cancellation), the projection index becomes invalid.
        if (!res)
            return nullptr;

        bitmaps.emplace_back(res);
    }

    ProjectionIndexBitmapPtr projection_index_bitmap;
    if (!bitmaps.empty())
    {
        projection_index_bitmap = std::move(bitmaps[0]);
        for (size_t i = 1; i < bitmaps.size(); ++i)
            projection_index_bitmap->intersectWith(*bitmaps[i]);
    }

    return projection_index_bitmap;
}

void MergeTreeProjectionIndexReader::cancel() noexcept
{
    for (auto && [_, reader] : projection_index_readers)
        reader.cancel();
}

MergeTreeIndexReadResultPool::MergeTreeIndexReadResultPool(
    MergeTreeSkipIndexReaderPtr skip_index_reader_, MergeTreeProjectionIndexReaderPtr projection_index_reader_)
    : skip_index_reader(std::move(skip_index_reader_))
    , projection_index_reader(std::move(projection_index_reader_))
{
    chassert(skip_index_reader || projection_index_reader);
}

MergeTreeIndexReadResultPtr
MergeTreeIndexReadResultPool::getOrBuildIndexReadResult(const RangesInDataPart & part, const RangesInDataParts & projection_parts)
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
            if (skip_index_reader)
            {
                auto skip_index_res = skip_index_reader->read(part);
                if (skip_index_res)
                {
                    res = std::make_shared<MergeTreeIndexReadResult>();
                    res->skip_index_read_result = std::move(skip_index_res);
                }
            }

            if (projection_index_reader)
            {
                auto projection_index_res = projection_index_reader->read(projection_parts);
                if (projection_index_res)
                {
                    if (!res)
                        res = std::make_shared<MergeTreeIndexReadResult>();
                    res->projection_index_read_result = std::move(projection_index_res);
                }
            }

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
    if (skip_index_reader)
        skip_index_reader->cancel();

    if (projection_index_reader)
        projection_index_reader->cancel();
}

}
