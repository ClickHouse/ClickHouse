#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <shared_mutex>
#include <roaring.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

struct ProjectionIndexBitmap;
using ProjectionIndexBitmapPtr = std::shared_ptr<ProjectionIndexBitmap>;
using ProjectionIndexBitmaps = std::vector<ProjectionIndexBitmapPtr>;
using ProjectionIndexBitmapsBuilder = std::function<ProjectionIndexBitmaps()>;

/// A bitmap wrapper for either 32-bit or 64-bit Roaring bitmaps, used in projection index processing.
struct ProjectionIndexBitmap
{
    enum class BitmapType
    {
        Bitmap32, // For roaring_bitmap_t (32-bit)
        Bitmap64 // For roaring64_bitmap_t (64-bit)
    };

    union BitmapUnion
    {
        roaring::api::roaring_bitmap_t * bitmap32;
        roaring::api::roaring64_bitmap_t * bitmap64;

        BitmapUnion()
            : bitmap32(nullptr)
        {
        }
    };

    BitmapType type;
    BitmapUnion data;

    explicit ProjectionIndexBitmap(BitmapType bitmap_type)
        : type(bitmap_type)
    {
        if (type == BitmapType::Bitmap32)
            data.bitmap32 = roaring::api::roaring_bitmap_create();
        else
            data.bitmap64 = roaring::api::roaring64_bitmap_create();
    }

    ~ProjectionIndexBitmap()
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

    static ProjectionIndexBitmapPtr create32() { return std::make_shared<ProjectionIndexBitmap>(BitmapType::Bitmap32); }

    static ProjectionIndexBitmapPtr create64() { return std::make_shared<ProjectionIndexBitmap>(BitmapType::Bitmap64); }

    template <typename Offset>
    static ProjectionIndexBitmapPtr createFromRange(std::type_identity_t<Offset> start, std::type_identity_t<Offset> end)
    {
        static_assert(
            std::is_same_v<Offset, UInt32> || std::is_same_v<Offset, UInt64>,
            "ProjectionIndexBitmap::createFromRange<> only supports UInt32 and UInt64");

        ProjectionIndexBitmapPtr bitmap;

        if constexpr (std::is_same_v<Offset, UInt32>)
        {
            bitmap = create32();
            roaring_bitmap_add_range_closed(bitmap->data.bitmap32, start, end - 1);
        }
        else
        {
            bitmap = create64();
            roaring64_bitmap_add_range_closed(bitmap->data.bitmap64, start, end - 1);
        }

        return bitmap;
    }

    void intersectWith(const ProjectionIndexBitmap & other) /// NOLINT
    {
        chassert(type == other.type);

        if (type == BitmapType::Bitmap32)
            roaring_bitmap_and_inplace(data.bitmap32, other.data.bitmap32);
        else
            roaring64_bitmap_and_inplace(data.bitmap64, other.data.bitmap64);
    }

    size_t cardinality() const
    {
        if (type == BitmapType::Bitmap32)
            return roaring_bitmap_get_cardinality(data.bitmap32);
        else
            return roaring64_bitmap_get_cardinality(data.bitmap64);
    }

    bool empty() const
    {
        if (type == BitmapType::Bitmap32)
            return roaring_bitmap_is_empty(data.bitmap32);
        else
            return roaring64_bitmap_is_empty(data.bitmap64);
    }

    template <typename Offset>
    bool contains(std::type_identity_t<Offset> value)
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
    void add(std::type_identity_t<Offset> value)
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

    /// Appends a `PaddedPODArray<UInt8>` in the range [starting_row, starting_row + num_rows)
    /// with `1`s at positions corresponding to values present in the bitmap.
    ///
    /// For example, if the bitmap contains values {101, 103} and `starting_row = 100`, `num_rows = 5`,
    /// the resulting buffer segment (of size `num_rows`) will be: [0, 1, 0, 1, 0].
    ///
    /// The method resizes the `filter` array by appending `num_rows` zero-initialized bytes to it,
    /// and writes `1` to positions where a row number is found in the bitmap.
    ///
    /// Returns true if at least one value in the bitmap falls within the specified range.
    template <typename Offset>
    bool appendToFilter(PaddedPODArray<UInt8> & filter, size_t starting_row, size_t num_rows) const
    {
        static_assert(
            std::is_same_v<Offset, UInt32> || std::is_same_v<Offset, UInt64>,
            "ProjectionIndexBitmap::fillBits<> only supports UInt32 and UInt64");

        size_t old_size = filter.size();
        filter.resize_fill(old_size + num_rows);
        UInt8 * pos = &filter[old_size];
        size_t ending_row = starting_row + num_rows;
        if constexpr (std::is_same_v<Offset, UInt32>)
        {
            chassert(type == BitmapType::Bitmap32);

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
};

/// A read pool specialized for reading from projection index parts. This pool coordinates the parallel execution of
/// projection index read tasks, and it is responsible for building and sharing projection index bitmaps across tasks
/// reading from the same data part.
///
/// Key Responsibilities:
/// - Serves as the unified access point for reading projection index data in a multi-threaded environment.
/// - Lazily builds the ProjectionIndexBitmap for a part on-demand, ensuring that only one thread performs
///   the construction, while others wait for the result via shared futures.
/// - Stores the constructed bitmap in a shared registry keyed by data part identity (raw pointer),
///   allowing concurrent tasks to reuse the result efficiently.
/// - Handles cleanup of bitmaps once the last read task for a part completes, releasing associated resources.
class MergeTreeReadPoolProjectionIndex : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolProjectionIndex(
        MutationsSnapshotPtr mutations_snapshot_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolProjectionIndex"; }
    bool preservesOrderOfRanges() const override { return true; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTaskPtr getTask(const RangesInDataPart & part);
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    /// Holds a shared future to a lazily built ProjectionIndexBitmap.
    /// This enables concurrent consumers to wait on a single computation.
    struct ProjectionIndexBitmapEntry
    {
        std::promise<ProjectionIndexBitmapPtr> promise;
        std::shared_future<ProjectionIndexBitmapPtr> future = promise.get_future().share();
    };

    /// Lazily constructs and caches the ProjectionIndexBitmap for a given data part. If the bitmap is already being
    /// built by another thread, waits for its result. Throws if the builder fails.
    ///
    /// This map uses raw pointer of data part as key because it is unique and stable for the lifetime of the part.
    ProjectionIndexBitmapPtr
    getProjectionIndexBitmap(const DataPartPtr & part, std::function<ProjectionIndexBitmapPtr()> builder) const
    {
        std::lock_guard lock(projection_index_bitmap_registry_mutex);
        auto it = projection_index_bitmap_registry.find(part.get());

        if (it == projection_index_bitmap_registry.end())
        {
            it = projection_index_bitmap_registry.emplace(part.get(), ProjectionIndexBitmapEntry{}).first;
            try
            {
                auto res = builder();
                it->second.promise.set_value(res);
                return res;
            }
            catch (...)
            {
                it->second.promise.set_value(nullptr);
                throw;
            }
        }
        else
        {
            return it->second.future.get();
        }
    }

    /// Cleans up the cached ProjectionIndexBitmap for a given part if it exists.
    /// Should be called when the last task for the part has finished.
    void cleanupProjectionIndexBitmap(const DataPartPtr & part) const
    {
        std::lock_guard lock(projection_index_bitmap_registry_mutex);
        projection_index_bitmap_registry.erase(part.get());
    }

    /// Stores ProjectionIndexBitmap instances per part to avoid redundant construction.
    mutable std::unordered_map<const IMergeTreeDataPart *, ProjectionIndexBitmapEntry> projection_index_bitmap_registry;
    mutable std::shared_mutex projection_index_bitmap_registry_mutex;
};

}
