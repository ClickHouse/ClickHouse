#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

struct MergeTreeIndexGranuleMinMax final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleMinMax(
        const String & index_name_,
        const Block & index_sample_block_,
        Ranges && hyperrectangle_);

    ~MergeTreeIndexGranuleMinMax() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return hyperrectangle.empty(); }

    size_t memoryUsageBytes() const override { return hyperrectangle.capacity() * sizeof(Range); }

    const String & index_name;
    const Block & index_sample_block;

    Ranges hyperrectangle;
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
    Ranges hyperrectangle;
};


struct MergeTreeIndexBulkGranulesMinMaxColumnar;
struct MergeTreeIndexConditionMinMaxTestAccess;

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

    /// Evaluate a columnar batch and return its surviving granule numbers.
    FilteredGranules getPossibleGranules(const MergeTreeIndexBulkGranulesPtr & idx_granules) const override;

    /// Whether this condition can be evaluated in bulk.
    bool hasBulkFastPath() const { return minmax_actions != nullptr; }

    /// Bulk omits per-leaf partial-disjunction bits, so it is compatible only when
    /// this index owns no leaf below an OR.
    bool bulkPreservesDisjunctionPrecision() const { return condition.everyDisjunctionIsOverUnownedLeaves(); }


    ~MergeTreeIndexConditionMinMax() override = default;
private:
    friend struct MergeTreeIndexConditionMinMaxTestAccess;

    Block executeBulkActions(const MergeTreeIndexBulkGranulesMinMaxColumnar & bulk) const;

    /// Bulk expression over paired min/max columns; null when the RPN cannot be lowered.
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
    MergeTreeIndexMinMax(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_)
        : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    {}

    ~MergeTreeIndexMinMax() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    /// Bulk filtering: see MergeTreeIndexBulkGranulesMinMaxColumnar. The caller (filterMarksUsingIndex)
    /// additionally gates this on the `use_minmax_index_bulk_filtering` setting.
    bool supportsBulkFiltering() const override { return true; }
    MergeTreeIndexBulkGranulesPtr createIndexBulkGranules() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override { return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}; }
    MergeTreeIndexFormat getDeserializedFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const override;
    MergeTreeIndexSubstreams getAllSubstreamsInPart(
        const MergeTreeDataPartChecksums & checksums,
        const std::string & path_prefix,
        const IDataPartStorage * storage) const override;
};

/// Columnar min/max values consumed by the bulk expression.
struct MergeTreeIndexBulkGranulesMinMaxColumnar final : public IMergeTreeIndexBulkGranules
{
    explicit MergeTreeIndexBulkGranulesMinMaxColumnar(const Block & index_sample_block_, size_t size_hint);
    void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) override;
    /// Optimized bulk read for fast-kind columns: one virtual call per chunk (instead of per
    /// granule) and a tight inner `readPODBinary` loop. For columns whose `fast_kind` is
    /// `None` (Nullable, Decimal, DateTime64, UUID, String, ...) this falls back to
    /// looping over the per-granule `deserializeBinary`.
    void deserializeBinaryBulk(size_t count, ReadBuffer & istr, MergeTreeIndexVersion version) override;

    /// Native type used by the raw-byte read path; `None` uses normal deserialization.
    enum class FastKind : UInt8
    {
        None = 0,
        U8, U16, U32, U64,
        I8, I16, I32, I64,
        F32, F64,
    };

    struct PerColumn
    {
        MutableColumnPtr min_col;
        MutableColumnPtr max_col;
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
