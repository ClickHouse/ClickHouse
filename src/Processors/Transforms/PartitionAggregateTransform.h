#pragma once

#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/IAccumulatingTransform.h>

#include <variant>

namespace DB
{

/** Computes aggregate window functions over whole partitions, `f(x) OVER (PARTITION BY keys)`, without
  * sorting. Keys are grouped by their bytes, so equal keys must have equal bytes (see
  * `keyTypeBreaksHashSharding`): fixed-size keys are packed into an integer of up to 32 bytes, other keys
  * are serialized. The input is buffered and spills under the external sort thresholds.
  *
  * A partition has a group: the states of the functions, and a number, which is appended to the buffered rows.
  * Few partitions are grouped as the rows come; with many, the rows are grouped at the end in buckets (see
  * `Grouping`), and a partition of one row takes no group (see `SingleRowStates`).
  */
class PartitionAggregateTransform final : public IAccumulatingTransform
{
public:
    struct SpillSettings
    {
        /// Spill the buffered rows when they, together with the keys and the aggregate states, exceed this,
        /// 0 to never spill.
        size_t max_bytes_before_external = 0;
        /// And the query uses more memory than this, 0 for no condition.
        size_t max_query_bytes_before_external = 0;
        size_t min_free_disk_space = 0;
        TemporaryDataOnDiskScopePtr tmp_data;
    };

    PartitionAggregateTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        ColumnNumbers key_positions_,
        std::vector<WindowFunctionDescription> functions_,
        SpillSettings spill_settings_);

    ~PartitionAggregateTransform() override;

    String getName() const override { return "PartitionAggregateTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// The group is stored as `UInt32` in the buffered and spilled rows. The maximum value marks a row that is the
    /// only one of its partition, and has no group: it is aggregated when it is output, see `SingleRowStates`.
    static constexpr UInt32 single_row_group = std::numeric_limits<UInt32>::max();
    static constexpr size_t max_groups = single_row_group;
    /// Up to this many partitions, the rows are grouped as they come, see `Grouping`.
    static constexpr size_t max_groups_to_group_eagerly = 1 << 16;
    static constexpr size_t num_buckets_bits = 8;
    /// See `aggregateChunk`.
    static constexpr size_t min_rows_per_group_to_aggregate_by_partitions = 16;
    static constexpr size_t max_groups_to_aggregate_by_partitions = 1 << 16;

    /// Grows twice instead of four times once the table is large, which halves its memory for many
    /// partitions: a partition is one cell, and each row of a stream can be a new partition.
    struct Grower : public HashTableGrowerWithPrecalculation<>
    {
        void increaseSize() { increaseSizeDegree(sizeDegree() >= 16 ? 1 : 2); }
    };

    /// Not a CRC hash, which the scatter by the partitions into the streams takes, so that the keys of a stream
    /// do not have the same bits of the hash.
    template <typename Key>
    using FixedKeyToGroup = HashMap<Key, UInt32, DefaultHash<Key>, Grower>;
    using SerializedKeyToGroup = HashMapWithSavedHash<std::string_view, UInt32, DefaultHash<std::string_view>, Grower>;
    template <typename Map>
    static constexpr bool is_serialized = std::is_same_v<Map, SerializedKeyToGroup>;

    /** Up to `max_groups_to_group_eagerly` partitions, the rows are grouped as they come, in one table that fits
      * in the cache. With more partitions, a table of all of them would take a cache miss for most rows, and
      * memory to grow through every size, so the rows that come later are deferred: each of them is put into a
      * bucket by the hash of its key, and at the end the keys are partitioned by the buckets and each bucket is
      * grouped in turn, in a small table that is reused.
      */
    template <typename Map>
    struct Grouping
    {
        /// The partitions of the rows grouped as they came.
        Map map;
        Map bucket_map;
    };

    ColumnRawPtrs getKeyColumns(const Columns & columns, Columns & holders) const;
    /// The arguments of each function, with `filter` and then `permutation` applied unless they are empty.
    std::vector<ColumnRawPtrs> getArguments(
        const Columns & columns, const IColumn::Filter & filter, size_t filtered_size, const IColumn::Permutation & permutation,
        Columns & holders) const;
    template <typename Key>
    void packKeys(const ColumnRawPtrs & key_columns, size_t num_rows, Key * keys) const;
    /// The key of a row, serialized into `pool` unless it is one `String`.
    std::string_view getSerializedKey(size_t row, const ColumnRawPtrs & key_columns, Arena & pool) const;
    /// Assigns the partitions of the rows of the buffered chunks that do not have them yet and aggregates them.
    /// If `last`, no more rows come, and the partitions of the deferred rows do not have to be remembered; the grouping
    /// then stops early if the query is cancelled.
    void groupChunks(bool last);
    template <typename Map>
    void addGroups(Map & map, size_t num_rows, const ColumnRawPtrs & key_columns, UInt32 * group_data);
    static size_t getBucket(size_t hash);
    template <typename Map>
    void deferChunk(Grouping<Map> & grouping, const Chunk & chunk);
    template <typename Map>
    void groupDeferred(Grouping<Map> & grouping, bool last);
    template <typename Map, typename KeyHolder>
    UInt32 emplaceGroup(Map & map, KeyHolder && key_holder, size_t hash);
    void aggregateChunk(Chunk & chunk, MutableColumnPtr groups);
    static void checkNumberOfGroups(size_t num_groups);
    /// The states of the rows of a chunk that are the only ones of their partitions, created when it is output.
    class SingleRowStates
    {
    public:
        explicit SingleRowStates(PartitionAggregateTransform & transform_) : transform(transform_) {}
        ~SingleRowStates();
        /// Aggregates the rows of `single_row_group`.
        void aggregate(const Columns & columns, const PaddedPODArray<UInt32> & group_data, size_t num_rows);
        AggregateDataPtr place(size_t row) const { return states + row * transform.state_stride; }

    private:
        PartitionAggregateTransform & transform;
        AggregateDataPtr states = nullptr;
        size_t num_created = 0;
    };

    void createStates(AggregateDataPtr place);
    void destroyStates(AggregateDataPtr place) const noexcept;
    UInt32 createGroup();
    /// Creates this many groups with the states next to each other, returns the first of them.
    size_t createGroups(size_t num_groups);
    void spill();
    /// Inserts the results of a function for these places.
    void insertResults(size_t function, AggregateDataPtr * result_places, size_t num_places, IColumn & to);

    const ColumnNumbers key_positions;
    const std::vector<WindowFunctionDescription> functions;
    const SpillSettings spill_settings;

    std::vector<ColumnNumbers> argument_positions;
    /// The states of all functions of a group are stored together, at these offsets.
    std::vector<size_t> state_offsets;
    size_t state_alignment = 1;
    /// The size of the states of a group, rounded up to their alignment.
    size_t state_stride = 0;
    /// A state with a non-trivial destructor owns memory, which can grow large.
    bool has_states_owning_memory = false;

    Arena arena;
    /// The sizes of the keys if they are packed, empty if they are serialized.
    std::vector<size_t> key_sizes;
    /// The key is one `String` or `LowCardinality(String)`, whose bytes are used as they are instead of being serialized.
    bool single_string_key = false;
    std::variant<
        Grouping<FixedKeyToGroup<UInt64>>,
        Grouping<FixedKeyToGroup<UInt128>>,
        Grouping<FixedKeyToGroup<UInt256>>,
        Grouping<SerializedKeyToGroup>> grouping;
    PaddedPODArray<AggregateDataPtr> places;
    PaddedPODArray<AggregateDataPtr> row_places;
    /// For `SingleRowStates`, reused for every chunk.
    PaddedPODArray<char> single_row_states_buffer;
    /// The number of partitions of one row, which have no group.
    size_t total_single_row_groups = 0;

    bool defer_grouping = false;
    /// The bucket of each deferred row.
    PaddedPODArray<UInt8> deferred_buckets;

    /// Input chunks, the first `num_grouped_chunks` of them with the group of every row appended as the last column.
    Chunks chunks;
    size_t num_grouped_chunks = 0;
    size_t chunks_bytes = 0;
    size_t next_chunk = 0;

    /// Constant columns are not written to the temporary files.
    std::vector<bool> is_const_column;
    SharedHeader spilled_header;
    /// A file per spill, like `MergeSortingTransform`, so that each one reserves the disk space it needs.
    std::vector<TemporaryBlockStreamHolder> spilled;
    size_t next_spilled = 0;
    std::optional<TemporaryBlockStreamReaderHolder> spilled_reader;

    size_t num_input_rows = 0;
    /// The results of the partitions, if they are not taken for each row.
    Columns results;
    bool results_for_each_row = false;
    bool results_ready = false;

};

}
