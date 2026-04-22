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
    extern const Event SelectedMarks;
    extern const Event SelectedRanges;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSkipIndexReader::MergeTreeSkipIndexReader(
    UsefulSkipIndexes skip_indexes_,
    std::optional<KeyCondition> & key_condition_rpn_template_,
    bool use_for_disjunctions_,
    MarkCachePtr mark_cache_,
    UncompressedCachePtr uncompressed_cache_,
    VectorSimilarityIndexCachePtr vector_similarity_index_cache_,
    MergeTreeReaderSettings reader_settings_,
    LoggerPtr log_)
    : skip_indexes(std::move(skip_indexes_))
    , key_condition_rpn_template(key_condition_rpn_template_)
    , use_for_disjunctions(use_for_disjunctions_)
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
    [[maybe_unused]] size_t total_granules = ranges.getNumberOfMarks();

    IndexGranulesMap index_granules;

    MergeTreeDataSelectExecutor::PartialDisjunctionResult partial_eval_results;
    if (use_for_disjunctions)
        partial_eval_results.resize(part.data_part->index_granularity->getMarksCountWithoutFinal() * MergeTreeDataSelectExecutor::MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT, true);
    for (const auto & index_and_condition : skip_indexes.useful_indices)
    {
        if (is_cancelled)
            return {};

        if (ranges.empty())
            break;

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

        auto [filtered_ranges, filtered_hints] = MergeTreeDataSelectExecutor::filterMarksUsingIndex(
            index_and_condition.index,
            index_and_condition.condition,
            key_condition_rpn_template,
            part.data_part,
            ranges,
            part.read_hints,
            reader_settings,
            mark_cache.get(),
            uncompressed_cache.get(),
            vector_similarity_index_cache.get(),
            use_for_disjunctions,
            partial_eval_results,
            log);

        ranges = std::move(filtered_ranges);

        for (auto & [name, granule] : filtered_hints.index_granules)
            index_granules[name] = std::move(granule);

        LOG_DEBUG(log, "Index {} has dropped {}/{} granules in part {}", index_and_condition.index->index.name,
                        (total_granules - ranges.getNumberOfMarks()), total_granules, part.data_part->name);
        total_granules = ranges.getNumberOfMarks();
    }

    if (use_for_disjunctions)
    {
        ranges = MergeTreeDataSelectExecutor::mergePartialResultsForDisjunctions(
                            part.data_part, ranges, key_condition_rpn_template.value(),
                            partial_eval_results, reader_settings, log);

        LOG_DEBUG(log, "Final set of granules after AND/OR processing : {} out of {} in part {}",
                        ranges.getNumberOfMarks(), total_granules, part.data_part->name);
        total_granules = ranges.getNumberOfMarks();
    }

    ProfileEvents::increment(ProfileEvents::SelectedMarks, ranges.getNumberOfMarks());
    ProfileEvents::increment(ProfileEvents::SelectedRanges, ranges.size());

    if (is_cancelled)
        return {};

    auto res = std::make_shared<SkipIndexReadResult>();
    res->granules_selected.resize(part.data_part->index_granularity->getMarksCountWithoutFinal(), false);
    for (const auto & range : ranges)
    {
        for (auto i = range.begin; i < range.end; ++i)
            (*res).granules_selected[i] = true;
    }

    res->index_granules = std::move(index_granules);

    if (skip_indexes.skip_index_for_top_k_filtering && skip_indexes.threshold_tracker)
    {
        res->min_max_index_for_top_k = MergeTreeDataSelectExecutor::getMinMaxIndexGranules(
            part.data_part,
            skip_indexes.skip_index_for_top_k_filtering,
            ranges,
            skip_indexes.threshold_tracker->getDirection(),
            true,/*access_by_mark*/
            reader_settings,
            mark_cache.get(),
            uncompressed_cache.get(),
            vector_similarity_index_cache.get());

        res->threshold_tracker = skip_indexes.threshold_tracker;
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
        if (!roaring_uint32_iterator_move_equalorlarger(&it, static_cast<UInt32>(begin)))
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
        if (!roaring_uint32_iterator_move_equalorlarger(&it, static_cast<UInt32>(starting_row)))
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

namespace
{

/// Cursor adapter over a 32-bit roaring iterator. `moveTo(target)` seeks the
/// iterator to the first value >= target in amortized O(1) (bitset containers)
/// or O(log) (array/run containers) -- critically, without walking the entries
/// between the current position and `target`.
///
/// Mirrors the duck-typed interface of `Roaring64Cursor` so `walkBitmapToMarkRanges`
/// can be templated over cursor type. Holds an embedded iterator (no heap
/// allocation); pass by reference, do not copy.
struct Roaring32Cursor
{
    roaring::api::roaring_uint32_iterator_t it;

    explicit Roaring32Cursor(const roaring::api::roaring_bitmap_t * bitmap)
    {
        roaring_iterator_init(bitmap, &it);
    }

    bool hasValue() const { return it.has_value; }
    UInt64 value() const { return it.current_value; }

    void moveTo(UInt64 target)
    {
        /// 32-bit bitmap is chosen only when all row offsets fit in UInt32, so
        /// the saturated target is safe. A target past UInt32::max() can only
        /// arise at end-of-part and we short-circuit before reaching here.
        chassert(target <= std::numeric_limits<UInt32>::max());
        roaring::api::roaring_uint32_iterator_move_equalorlarger(&it, static_cast<UInt32>(target));
    }
};

/// Cursor adapter over a 64-bit roaring iterator. Heap-allocated via
/// `roaring64_iterator_create`; freed in the destructor.
struct Roaring64Cursor
{
    roaring::api::roaring64_iterator_t * it;

    explicit Roaring64Cursor(const roaring::api::roaring64_bitmap_t * bitmap)
        : it(roaring::api::roaring64_iterator_create(bitmap))
    {
        if (!it)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failed to allocate roaring64 iterator");
    }

    Roaring64Cursor(const Roaring64Cursor &) = delete;
    Roaring64Cursor & operator=(const Roaring64Cursor &) = delete;

    ~Roaring64Cursor() { roaring::api::roaring64_iterator_free(it); }

    bool hasValue() const { return roaring::api::roaring64_iterator_has_value(it); }
    UInt64 value() const { return roaring::api::roaring64_iterator_value(it); }
    void moveTo(UInt64 target) { roaring::api::roaring64_iterator_move_equalorlarger(it, target); }
};

/// Walk a roaring bitmap, emitting one mark range per granule that contains at
/// least one hit. The work is O(number_of_hit_granules), not O(cardinality):
/// after emitting a mark we seek the cursor past that granule's row range so a
/// dense cluster inside one granule costs one probe instead of one step per
/// entry.
///
/// Two granularity paths:
///   - Constant: `mark = offset / gr`; seek to `(mark + 1) * gr`. One divide,
///     no index-side lookups.
///   - Adaptive: keep a monotonic cursor over the part's mark rows; since the
///     bitmap emits values in ascending order we only ever advance forward
///     through `marks_rows_partial_sums`, amortising per-offset `lower_bound`
///     away.
template <typename Cursor>
void walkBitmapToMarkRanges(
    Cursor & cursor,
    const MergeTreeIndexGranularity & index_granularity,
    MarkRanges & result)
{
    const size_t num_marks = index_granularity.getMarksCount();
    if (num_marks == 0 || !cursor.hasValue())
        return;

    auto append_mark = [&](size_t mark)
    {
        chassert(mark < num_marks);
        /// Only coalesces with the immediately adjacent mark. Seek-distance
        /// coalescing (merging across sub-`min_marks_for_seek` gaps) is
        /// deliberately left to `intersectMarkRanges`, so this result stays
        /// independent of any reader settings and can be cached per-part via
        /// `std::call_once`.
        if (!result.empty() && result.back().end == mark)
            result.back().end = mark + 1;
        else
            result.emplace_back(mark, mark + 1);
    };

    if (auto constant_opt = index_granularity.getConstantGranularity(); constant_opt.has_value())
    {
        const size_t gr = *constant_opt;
        chassert(gr > 0);
        while (cursor.hasValue())
        {
            const size_t mark = std::min<size_t>(cursor.value() / gr, num_marks - 1);
            append_mark(mark);

            if (mark + 1 == num_marks)
                break; /// No granule past the last; any later hits are bogus.

            cursor.moveTo(static_cast<UInt64>(mark + 1) * gr);
        }
        return;
    }

    /// Adaptive: monotonic cursor through marks, driven by the bitmap's ascending
    /// offsets.
    size_t current_mark = 0;
    size_t current_mark_end_row = index_granularity.getMarkRows(0);
    while (cursor.hasValue())
    {
        const UInt64 offset = cursor.value();
        while (offset >= current_mark_end_row && current_mark + 1 < num_marks)
        {
            ++current_mark;
            current_mark_end_row += index_granularity.getMarkRows(current_mark);
        }
        append_mark(current_mark);

        if (current_mark + 1 == num_marks)
            break;

        cursor.moveTo(current_mark_end_row);
    }
}

}

MarkRanges bitmapToMarkRanges(
    const ProjectionIndexBitmap & bitmap,
    const MergeTreeIndexGranularity & index_granularity)
{
    MarkRanges result;

    if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
    {
        Roaring32Cursor cursor(bitmap.data.bitmap32);
        walkBitmapToMarkRanges(cursor, index_granularity, result);
    }
    else
    {
        Roaring64Cursor cursor(bitmap.data.bitmap64);
        walkBitmapToMarkRanges(cursor, index_granularity, result);
    }

    return result;
}

const MarkRanges & MergeTreeIndexReadResult::getProjectionMarkRanges(const MergeTreeIndexGranularity & index_granularity) const
{
    std::call_once(projection_mark_ranges_once, [&]
    {
        projection_mark_ranges = bitmapToMarkRanges(*projection_index_read_result, index_granularity);
    });
    return projection_mark_ranges;
}

MergeTreeIndexReadResultPtr lookupProjectionIndexResult(
    const MergeTreeIndexBuildContext & index_build_context,
    size_t part_index_in_query)
{
    auto part_it = index_build_context.read_ranges.find(part_index_in_query);
    if (part_it == index_build_context.read_ranges.end())
        return nullptr;

    auto proj_it = index_build_context.projection_read_ranges.find(part_index_in_query);
    if (proj_it == index_build_context.projection_read_ranges.end())
        return nullptr;

    auto index_result = index_build_context.index_reader_pool->getOrBuildIndexReadResult(
        part_it->second, proj_it->second);

    if (!index_result || !index_result->projection_index_read_result)
        return nullptr;

    return index_result;
}

MarkRanges narrowMarkRangesByProjectionIndex(
    const MergeTreeIndexReadResultPtr & index_result,
    const MergeTreeIndexGranularity & index_granularity,
    MarkRanges mark_ranges,
    size_t min_marks_for_seek)
{
    if (!index_result || !index_result->projection_index_read_result)
        return mark_ranges;

    /// Convert the bitmap to MarkRanges once per part and cache on the result object --
    /// subsequent getTask batches for the same part reuse the converted ranges. The
    /// cache stays seek-threshold-free; `intersectMarkRanges` applies coalescing to the
    /// per-batch intersection output instead.
    const auto & bitmap_ranges = index_result->getProjectionMarkRanges(index_granularity);
    return intersectMarkRanges(mark_ranges, bitmap_ranges, min_marks_for_seek);
}

MarkRanges narrowMarkRangesByProjectionIndex(
    const MergeTreeIndexBuildContext & index_build_context,
    size_t part_index_in_query,
    const MergeTreeIndexGranularity & index_granularity,
    MarkRanges mark_ranges,
    size_t min_marks_for_seek)
{
    auto index_result = lookupProjectionIndexResult(index_build_context, part_index_in_query);
    return narrowMarkRangesByProjectionIndex(
        index_result, index_granularity, std::move(mark_ranges), min_marks_for_seek);
}

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
          IndexReadTasks{} /*index_read_tasks*/,
          actions_settings,
          reader_settings))
{
}

ProjectionIndexBitmapPtr SingleProjectionIndexReader::read(const RangesInDataPart & ranges)
{
    bool can_use_32bit_part_offset = ranges.parent_ranges.max_part_offset <= std::numeric_limits<UInt32>::max();

    auto task = projection_index_read_pool->getTask(ranges);
    MergeTreeProjectionIndexSelectAlgorithm algorithm;
    auto res = can_use_32bit_part_offset ? ProjectionIndexBitmap::create32() : ProjectionIndexBitmap::create64();

    /// Start reading chunks from the projection index reader.
    /// Each chunk contains a column of UInt64 offsets that we insert into the bitmap.
    while (!processor->is_cancelled && !task->isFinished())
    {
        auto chunk = processor->readCurrentTask(*task, algorithm);
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
                            res->add<Offset>(static_cast<Offset>(offset));
                    }
                    else
                    {
                        for (auto offset : offsets.getData())
                        {
                            if (ranges.parent_ranges.contains(offset))
                                res->add<Offset>(static_cast<Offset>(offset));
                        }
                    }
                };
                if (can_use_32bit_part_offset)
                    add_offsets(UInt32{});
                else
                    add_offsets(UInt64{});
            }
        }
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
