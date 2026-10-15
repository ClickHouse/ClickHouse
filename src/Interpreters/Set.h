#pragma once

#include <QueryPipeline/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/SetKeys.h>
#include <Core/PlainRanges.h>
#include <Storages/MergeTree/BoolMask.h>

#include <Common/callOnce.h>
#include <Common/SharedMutex.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ProcessorMemoryStats.h>
#include <Interpreters/castColumn.h>


namespace DB
{

struct Range;
using Ranges = VectorWithMemoryTracking<Range>;

class Context;
class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;
using Sizes = std::vector<size_t>;

struct ColumnWithTypeAndName;
using ColumnsWithTypeAndName = VectorWithMemoryTracking<ColumnWithTypeAndName>;

class Chunk;
class DiskSet;
class DiskSetBuilder;
class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/// These settings decide when and where a `Set` spills to disk, as described at `Set::setSpillSettings`.
/// The defaults keep the set in memory.
struct SetSpillSettings
{
    /// The threshold of tracked query memory for automatic spilling. With 0, only explicit requests spill.
    size_t max_bytes_before_external_set = 0;

    /// Holds the runs of the external sort and the finished set.
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t min_free_disk_space = 0;

    /// This is the number of rows in the blocks that the external sort merges and in the moved key columns.
    size_t max_block_size = 0;
    QueryStatusPtr process_list_element;
};

/** Data structure for implementation of IN expression.
  */
class Set
{
public:
    /// 'fill_set_elements': in addition to hash table
    /// (that is useful only for checking that some value is in the set and may not store the original values),
    /// store all set elements in explicit form.
    /// This is needed for subsequent use for index.
    Set(const SizeLimits & limits_, size_t max_elements_to_fill_, bool transform_null_in_);

    ~Set();

    /// Configures when the set spills to disk. Call it before inserting rows. With a nonzero threshold, the
    /// set spills once it outgrows the threshold of tracked query memory: before inserting a chunk whose
    /// projected growth, plus the memory that spilling needs, exceeds the memory left under the threshold, or
    /// after inserting a chunk when query memory exceeds the threshold. Either way, the set spills only once
    /// its keys take as much memory as the smallest run of its builder, the smaller of 16 MiB and the
    /// threshold. The keys then go to a `DiskSet`, and explicit elements are dropped, as when there are more
    /// of them than `max_elements_to_fill`.
    /// Once on disk, the size limits apply to the distinct keys and to the memory of the set when the keys
    /// are merged into the file, so the set receives every chunk. In the `break` overflow mode, it then keeps
    /// the keys that come first in the order of their disk keys rather than the first ones to arrive. Disk
    /// keys compare as unsigned integers, so this order matches the order of values only for a single key
    /// stored as an unsigned integer, such as `UInt64`, `Date` or `DateTime`.
    /// A set that stays in memory while it is filled is usually built before the rest of the query takes its
    /// memory, so it can also spill while it is used: when `execute` finds query memory above the threshold
    /// and the keys large enough to spill, the keys are written to a `DiskSet` while other lookups keep
    /// reading the table, and the disk set then replaces the table. The keys are final by then, so no size
    /// limit applies, and explicit elements stay, because their consumers read them without locking the set.
    /// With temporary storage configured, `spill` also accepts explicit scheduler requests in either phase,
    /// independently of the automatic threshold.
    void setSpillSettings(SetSpillSettings spill_settings_);

    ProcessorMemoryStats getMemoryStats() const;
    size_t spill(size_t at_least_bytes);
    const TemporaryDataOnDiskScope * getSpillScope() const { return spill_settings.tmp_data.get(); }

    bool transformNullIn() const { return transform_null_in; }

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    void setHeader(const ColumnsWithTypeAndName & header);

    /// Returns false, if some limit was exceeded and no need to insert more data.
    bool insertFromColumns(const Columns & columns);
    bool insertFromBlock(const ColumnsWithTypeAndName & columns);

    void fillSetElements();
    bool insertFromColumns(const Columns & columns, SetKeyColumns & holder);
    void appendSetElements(SetKeyColumns & holder);

    /// Call after all blocks were inserted. To get the information that set is already created.
    void finishInsert();

    /// finishInsert and isCreated are thread-safe
    bool isCreated() const { return is_created.load(); }

    /// Whether the set building was stopped early because of size limits with OverflowMode::BREAK.
    bool isTruncated() const { return is_truncated.load(); }

    void checkIsCreated() const;

    void processDateTime64Column(const ColumnWithTypeAndName & column_to_cast, ColumnPtr & result, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map) const;

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      * A set in memory can spill to disk during the call, as described at `setSpillSettings`.
      */
    ColumnPtr execute(const ColumnsWithTypeAndName & columns, bool negative) const;

    bool hasNull() const;

    bool empty() const;
    size_t getTotalRowCount() const;
    size_t getTotalByteCount() const;

    const DataTypes & getDataTypes() const { return data_types; }
    const DataTypes & getElementsTypes() const { return set_elements_types; }

    bool hasExplicitSetElements() const { return fill_set_elements || (!set_elements.empty() && set_elements.front()->size() == data->getTotalRowCount()); }
    bool hasSetElements() const { return !set_elements.empty(); }
    Columns getSetElements() const;

    /// The elements of a single-column set viewed as sorted, non-overlapping ranges, built once and
    /// shared afterwards. Deriving them costs one `Field` per element plus an O(N log N) sort, which is
    /// substantial for a large set, and every consumer of the same set derives exactly the same value —
    /// notably the two plan builds that automatic parallel replicas performs for one query.
    /// Returns null for a multi-column (tuple) set, which has no single-column range representation.
    std::shared_ptr<const PlainRanges> getPlainRanges() const;

    void checkColumnsNumber(size_t num_key_columns) const;
    bool areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;
    void checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;

    static DataTypes getElementTypes(DataTypes types, bool transform_null_in);

    /// Limitations on the maximum size of the set
    const SizeLimits limits;

    /// If true, insert NULL values to set.
    const bool transform_null_in;

    const size_t max_elements_to_fill;

private:
    size_t keys_size = 0;
    Sizes key_sizes;

    /// This holds the keys in memory. Spilling releases the table and its arena. Spilling while the set is
    /// used changes how the set stores its keys but not which keys it has, so `execute` stays `const`, and
    /// the members that `spillAfterBuild` replaces are `mutable`.
    mutable std::unique_ptr<SetVariants> data = std::make_unique<SetVariants>();

    /// They are kept after the build, so the set can spill while it is used.
    SetSpillSettings spill_settings;

    /// A set that spilled keeps its keys in the builder while it is filled, then in the finished disk set.
    std::unique_ptr<DiskSetBuilder> disk_set_builder;
    mutable std::unique_ptr<DiskSet> disk_set;

    /// Computes the disk keys of key columns. It is selected when the set spills.
    using ComputeDiskSetKeys = ColumnPtr (*)(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
    mutable ComputeDiskSetKeys compute_disk_set_keys = nullptr;

    /// Set by the first lookup that finds that the set has to spill while it is used, and never reset. Only
    /// that lookup spills the set.
    mutable std::atomic<bool> spill_after_build_started = false;

    /** How IN works with Nullable types.
      *
      * For simplicity reasons, all NULL values and any tuples with at least one NULL element are ignored in the Set.
      * And for left hand side values, that are NULLs or contain any NULLs, we return 0 (means that element is not in Set).
      *
      * If we want more standard compliant behaviour, we must return NULL
      *  if lhs is NULL and set is not empty or if lhs is not in set, but set contains at least one NULL.
      * It is more complicated with tuples.
      * For example,
      *      (1, NULL, 2) IN ((1, NULL, 3)) must return 0,
      *  but (1, NULL, 2) IN ((1, 1111, 2)) must return NULL.
      *
      * We have not implemented such sophisticated behaviour.
      */

    /** The data types from which the set was created.
      * When checking for belonging to a set, the types of columns to be checked must match with them.
      */
    DataTypes data_types;

    /// Types for set_elements.
    DataTypes set_elements_types;

    LoggerPtr log;

    /// Do we need to additionally store all elements of the set in explicit form for subsequent use for index.
    bool fill_set_elements = false;

    /// Check if set contains all the data.
    std::atomic<bool> is_created = false;

    /// Whether the set was truncated due to overflow with OverflowMode::BREAK.
    std::atomic<bool> is_truncated = false;

    /// Collected elements of `Set`.
    /// It is necessary for the index to work on the primary key in the IN statement.
    MutableColumns set_elements;

    mutable std::shared_ptr<const PlainRanges> plain_ranges;
    mutable OnceFlag plain_ranges_once;

    /// Protects the keys of the set. `insertFromBlock` takes it exclusively and `execute` shares it; they
    /// run at the same time only for `StorageSet`, whose inserts can run while queries use the set.
    /// Scheduler spills also take it exclusively while building, including explicit-element updates.
    /// `spillAfterBuild` takes it exclusively to replace the table with the disk set.
    mutable SharedMutex rwlock;

    /// A cache for cast functions (if any) to avoid rebuilding cast functions
    /// for every call to `execute`
    mutable std::unique_ptr<InternalCastFunctionCache> cast_cache;

    bool insertFromColumnsImpl(const Columns & columns, SetKeyColumns & holder);
    ProcessorMemoryStats getMemoryStatsUnlocked() const;

    template <typename Method>
    void insertFromBlockImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map,
        ColumnUInt8::Container * out_filter);

    template <typename Method, bool has_null_map, bool build_filter>
    void insertFromBlockImplCase(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map,
        ColumnUInt8::Container * out_filter);

    template <typename Method>
    void executeImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    template <typename Method, bool has_null_map>
    void executeImplCase(
        Method & method,
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    /// Returns whether the keys of the set, its table and arena, take at least the smallest run of its
    /// builder (see `DiskSetBuilder::getMinBytesInRun`). Spilling fewer keys would release less
    /// memory than that, part of which the builder would hold again until it writes a run, while lookups
    /// would read the disk, and a set that spills while it is filled would lose its explicit elements for
    /// index analysis.
    bool isLargeEnoughToSpill() const;

    /// Returns whether the set in memory has to spill before inserting `rows` keys: the set is large enough
    /// to spill, and the insertion could need more memory than the query has left under the spill threshold.
    /// The set then spills to disk before the insertion, because a single insertion can overshoot the
    /// threshold (e.g. hash table resize).
    bool isSpillNeededBeforeInsert(const ColumnRawPtrs & key_columns, size_t rows) const;

    /// Returns whether the set in memory has to spill now: query memory exceeds the spill threshold, and the
    /// set is large enough to spill. The set checks it after inserting a chunk and after each lookup in
    /// memory. Call it under `rwlock`.
    bool isSpillNeeded() const;

    /// Moves the keys of the table to a `DiskSetBuilder`, which takes later insertions, and frees the table.
    void spill(std::string_view reason, bool force = false);

    /// Writes the keys of the table to a `DiskSet` while lookups keep reading the table, then replaces the
    /// table with the disk set under the exclusive lock. Only the first caller spills, and the others return.
    /// Call it without holding `rwlock`.
    size_t spillAfterBuild(std::string_view reason, bool force = false) const;

    /// Logs that the set switches to disk, at which `stage` ("while it is built" or "while it is used") and why.
    void logSpill(std::string_view stage, std::string_view reason) const;

    size_t getDiskSetKeyBytes() const;

    /// Creates a `DiskSetBuilder` with the spill settings of the set and adds the keys of the table to it.
    template <typename Method>
    std::unique_ptr<DiskSetBuilder> createDiskSetBuilderWithKeys(
        const Method & method, const SizeLimits & builder_limits, bool force = false) const;

    void executeDiskSet(const ColumnRawPtrs & key_columns, ColumnUInt8::Container & vec_res, bool negative, ConstNullMapPtr null_map) const;
};

using SetPtr = std::shared_ptr<Set>;
using ConstSetPtr = std::shared_ptr<const Set>;
using Sets = std::vector<SetPtr>;


/// Class for checkInRange function.
class MergeTreeSetIndex
{
public:
    /** Mapping for tuple positions from Set::set_elements to
      * position of pk index and functions chain applied to this column.
      */
    struct KeyTuplePositionMapping
    {
        size_t tuple_index{};
        size_t key_index{};
        std::vector<FunctionBasePtr> functions;
    };

    MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_);

    size_t size() const { return ordered_set.at(0)->size(); }

    bool hasMonotonicFunctionsChain() const;

    BoolMask checkInRange(const Ranges & key_ranges, const DataTypes & data_types, bool single_point = false) const;

    /// Optimized overload. Instead of all/prefix of key columns, any subsequence of key column information (in order) can be given.
    /// `key_col_to_sparse_pos` maps key index to position in `sparse_hyperrectangle`, or -1 if not tracked.
    /// If some key column >= `key_col_to_sparse_pos`.size(), it is considered as not tracked.
    /// See KeyCondition::checkInRange for explanation of relevant parameters.
    BoolMask checkInRange(const std::vector<int> & key_col_to_sparse_pos, const Ranges & sparse_key_ranges, const DataTypes & sparse_data_types, bool single_point = false) const;

    const Columns & getOrderedSet() const { return ordered_set; }

    const std::vector<KeyTuplePositionMapping> & getIndexesMapping() const { return indexes_mapping; }

private:
    /** Class that represents single value with possible infinities.
      * Single field is stored in column for more optimal inplace comparisons with other regular columns.
      * Extracting fields from columns and further their comparison is suboptimal and requires extra copying.
      */
    struct FieldValue
    {
        explicit FieldValue(MutableColumnPtr && column_, bool block_memory_tracker_ = false)
            : column(std::move(column_)), block_memory_tracker(block_memory_tracker_) {}
        void update(const Field & x);

        bool isNormal() const { return !value.isPositiveInfinity() && !value.isNegativeInfinity(); }
        bool isPositiveInfinity() const { return value.isPositiveInfinity(); }
        bool isNegativeInfinity() const { return value.isNegativeInfinity(); }

        Field value; // Null, -Inf, +Inf

        // If value is Null, uses the actual value in column
        MutableColumnPtr column;

        /// True when the column belongs to the thread-local buffer of getFieldValueRangesBuffer,
        /// which outlives the query; its reallocations must not be charged to the current query.
        bool block_memory_tracker = false;
    };

    struct FieldValueRange
    {
        FieldValue left;
        FieldValue right;
        bool left_included = false;
        bool right_included = false;

        explicit FieldValueRange(const IColumn & prototype, bool block_memory_tracker = false)
            : left(prototype.cloneEmpty(), block_memory_tracker), right(prototype.cloneEmpty(), block_memory_tracker) {}
    };

    using FieldValueRanges = std::vector<FieldValueRange>;

    /// Buffer reused across checkInRange calls (which run once per mark during index analysis)
    /// to avoid per-call column allocations. For fixed-width key types it is a per-thread cache
    /// (invalidated by the next call on the same thread) whose retained size is small and bounded.
    /// Variable-width key types (e.g. String) could pin arbitrarily large reserved capacity in
    /// thread-local storage after the query ends, so for them the given query-scoped scratch
    /// buffer is filled and returned instead.
    FieldValueRanges & getFieldValueRangesBuffer(FieldValueRanges & scratch) const;

    // If all arguments in tuple are key columns, we can optimize NOT IN when there is only one element.
    bool has_all_keys;
    Columns ordered_set;
    std::vector<KeyTuplePositionMapping> indexes_mapping;
    const UInt64 instance_id;
    /// Whether all key columns have fixed-width values, making the ranges buffer cacheable.
    bool cache_ranges = false;
};

}
