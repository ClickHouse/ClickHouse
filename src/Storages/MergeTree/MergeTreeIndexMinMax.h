#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

/// `ExpressionActionsPtr` (used below only as a member pointer) is forward-declared via
/// `KeyCondition.h`; the heavy `Interpreters/ExpressionActions.h` is included in the `.cpp`
/// to keep this relatively high-fanout header light.


namespace DB
{

struct MergeTreeIndexGranuleMinMax final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleMinMax(
        const String & index_name_,
        const Block & index_sample_block_,
        std::vector<Range> && hyperrectangle_);

    ~MergeTreeIndexGranuleMinMax() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return hyperrectangle.empty(); }

    size_t memoryUsageBytes() const override { return hyperrectangle.capacity() * sizeof(Range); }

    const String & index_name;
    const Block & index_sample_block;

    std::vector<Range> hyperrectangle;
    Serializations serializations;
    DataTypes datatypes;
    FormatSettings format_settings;
};


struct MergeTreeIndexAggregatorMinMax final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorMinMax(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorMinMax() override = default;

    bool empty() const override { return hyperrectangle.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    std::vector<Range> hyperrectangle;
};


class MergeTreeIndexConditionMinMax final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionMinMax(
        const IndexDescription & index,
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;

    std::string getDescription() const override;

    /// Bulk entry point: evaluates the condition on every granule held by the provided bulk
    /// container in a single column-wise pass via the pre-built ActionsDAG. Returns the sorted
    /// list of surviving granule numbers. When the DAG couldn't be built (unsupported RPN
    /// shape), returns "all granules pass" and the caller is expected to have already opted
    /// out via `hasBulkFastPath()`.
    FilteredGranules getPossibleGranules(const MergeTreeIndexBulkGranulesPtr & idx_granules) const override;

    /// Whether the ActionsDAG-based bulk fast path is available. When true,
    /// `getPossibleGranules` will produce useful per-granule filtering; when false, the
    /// bulk call would just return "all granules pass" and the caller should skip it to
    /// avoid paying deserialization cost for no benefit.
    bool hasBulkFastPath() const { return minmax_actions != nullptr; }

    /// Whether the per-index condition's RPN is a pure conjunction of leaves (no
    /// FUNCTION_OR). Used to decide if bulk evaluation is precision-preserving when
    /// running alongside `use_skip_indexes_for_disjunctions`: bulk does not populate
    /// the partial-disjunction bitset, so any leaves this index owns stay at the
    /// bitset's `true` default. When the projected per-index condition is AND-only,
    /// those defaults coincide with the values per-granule evaluation would have
    /// written, and merge precision is preserved.
    bool indexConditionHasOnlyConjunctions() const { return condition.hasOnlyConjunctions(); }


    ~MergeTreeIndexConditionMinMax() override = default;
private:
    /// Pre-built ExpressionActions that evaluates the KeyCondition against paired
    /// (min_c, max_c) columns per index column, producing two UInt8 output columns
    /// `__minmax_can_be_true` and `__minmax_can_be_false`. Non-null when every RPN
    /// element was representable as a column-engine expression (no monotonic chains,
    /// no space-filling curves, no polygon, no relaxed predicates, no `FUNCTION_IN_SET`
    /// that didn't collapse to a single range, no bloom filter).
    ///
    /// This is the fast path that powers `getPossibleGranules` (bulk) and optionally
    /// `mayBeTrueOnGranule` (scalar, 1-row block). When null, the caller falls back
    /// to the generic `Field`-based path.
    ExpressionActionsPtr minmax_actions;
    /// For each index column, the paired input names in the DAG, in order.
    std::vector<std::pair<String, String>> minmax_input_names;
    /// Names of the two output UInt8 columns produced by `minmax_actions`.
    static constexpr const char * OUTPUT_CAN_BE_TRUE = "__minmax_can_be_true";
    static constexpr const char * OUTPUT_CAN_BE_FALSE = "__minmax_can_be_false";

    DataTypes index_data_types;
    KeyCondition condition;
};


class MergeTreeIndexMinMax : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexMinMax(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexMinMax() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    /// Bulk filtering: see MergeTreeIndexBulkGranulesMinMaxFast. The caller (filterMarksUsingIndex)
    /// additionally gates this on the `use_minmax_index_bulk_filtering` setting.
    bool supportsBulkFiltering() const override { return true; }
    MergeTreeIndexBulkGranulesPtr createIndexBulkGranules() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override { return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}; }
    MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override; /// NOLINT
};

/// Bulk container for minmax skip indexes: holds the min and the max of every deserialized granule
/// as columns (one row per granule, one pair of columns per index column), plus side flags for
/// granules whose min/max were serialized as NULL (version-2 NULL_LAST, treated as +∞).
///
/// Paired with `MergeTreeIndexConditionMinMax::getPossibleGranules`, which walks the RPN
/// element-by-element and evaluates each FUNCTION_IN_RANGE / FUNCTION_NOT_IN_RANGE as a vectorized
/// compare across all granules. AND/OR/NOT are byte-wise mask ops over per-granule BoolMasks.
struct MergeTreeIndexBulkGranulesMinMaxFast final : public IMergeTreeIndexBulkGranules
{
    explicit MergeTreeIndexBulkGranulesMinMaxFast(const Block & index_sample_block_, size_t size_hint);
    void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) override;
    /// Optimized bulk read for fast-kind columns: one virtual call per chunk (instead of per
    /// granule) and a tight inner `readPODBinary` loop. For columns whose `fast_kind` is
    /// `None` (Nullable, Decimal, DateTime64, UUID, String, ...) this falls back to
    /// looping over the per-granule `deserializeBinary`.
    void deserializeBinaryBulk(size_t count, ReadBuffer & istr, MergeTreeIndexVersion version) override;

    /// Which native type we can read with `readPODBinary` straight into
    /// `ColumnVector<T>::Container`, skipping the `Field` round-trip. Populated at
    /// `PerColumn` construction time; stays `None` for columns we can't (or won't) fast-read
    /// (`Nullable`, `Decimal`, `String`, `UUID`, `DateTime64`, `IPv4/6`, ...).
    enum class FastKind : UInt8
    {
        None = 0,
        U8, U16, U32, U64,
        I8, I16, I32, I64,
        F32, F64,
    };

    struct PerColumn
    {
        /// Columns of the index column's exact DataType, one row per deserialized granule.
        MutableColumnPtr min_col;
        MutableColumnPtr max_col;
        /// Side flags for granules whose min/max were serialized as NULL (v2 format maps
        /// all-NULL granules to POSITIVE_INFINITY). Currently unused by the DAG bulk path,
        /// which assumes no-NULL granules; see RESULTS.md for the known limitation.
        PaddedPODArray<UInt8> min_is_neg_inf;
        PaddedPODArray<UInt8> min_is_pos_inf;
        PaddedPODArray<UInt8> max_is_neg_inf;
        PaddedPODArray<UInt8> max_is_pos_inf;
        /// Non-`None` enables the raw-bytes read path in `deserializeBinary`.
        FastKind fast_kind = FastKind::None;
    };

    Block index_sample_block;
    DataTypes datatypes;
    Serializations serializations;
    FormatSettings format_settings;
    std::vector<PerColumn> cols;
    /// Number of granules appended so far. Granule numbers are implicit row positions 0..size-1.
    size_t size = 0;
};

struct MergeTreeIndexBulkGranulesMinMax final : public IMergeTreeIndexBulkGranules
{
    struct MinMaxGranule
    {
        size_t granule_num;
        Field min_or_max_value;
    };

    struct MinMaxGranuleItem
    {
        int direction;
        size_t part_index;
        size_t granule_num;
        Field min_or_max_value;
        /// If sort by ASC, then max-heap of min values, if sort by DESC, min-heap of max values
        bool operator < (const MinMaxGranuleItem & b) const
        {
            return (direction == 1 ? (min_or_max_value < b.min_or_max_value) : (min_or_max_value > b.min_or_max_value));
        }
    };

    explicit MergeTreeIndexBulkGranulesMinMax(const String & index_name_, const Block & index_sample_block_,
                                              size_t index_granularity_, int direction_, size_t size_hint_, size_t last_part_granule_, bool store_map_ = false);
    void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) override;

    void getTopKMarks(size_t n, bool handle_ties, std::vector<MinMaxGranule> & result);
    static void getTopKMarks(int direction, size_t n, size_t index_granularity, bool handle_ties,
                                const std::vector<std::vector<MinMaxGranule>> & parts, std::vector<MarkRanges> & result);

    std::vector<MinMaxGranule> granules;
    std::unordered_map<size_t, size_t> granules_map;

private:
    template<bool handle_ties>
    void getTopKMarks(size_t n, std::vector<MinMaxGranule> & result);

    template<bool handle_ties>
    static void getTopKMarks(int direction, size_t n, size_t index_granularity, const std::vector<std::vector<MinMaxGranule>> & parts, std::vector<MarkRanges> & result);

    SerializationPtr serialization;
    [[maybe_unused]] const String & index_name;
    const Block & index_sample_block;
    FormatSettings format_settings;
    size_t index_granularity;
    int direction;
    size_t last_part_granule;
    bool empty = true;
    bool store_map = false;
};

using MergeTreeIndexBulkGranulesMinMaxPtr = std::shared_ptr<MergeTreeIndexBulkGranulesMinMax>;

}
