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

    ~MergeTreeIndexConditionMinMax() override = default;
private:
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

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override { return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}; }
    MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override; /// NOLINT
};

struct MergeTreeIndexBulkGranulesMinMax final : public IMergeTreeIndexBulkGranules
{
    /// Mode of operation:
    /// - TopK: for ORDER BY LIMIT optimization, stores per-granule min or max values
    /// - Aggregate: for skip index aggregation, aggregates min and max across all granules
    enum class Mode : uint8_t { TopK, Aggregate };

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

    /// Constructor for TopK mode
    explicit MergeTreeIndexBulkGranulesMinMax(const String & index_name_, const Block & index_sample_block_,
                                              int direction_, size_t size_hint_, bool store_map_ = false);

    /// Constructor for Aggregate mode
    explicit MergeTreeIndexBulkGranulesMinMax(const Block & index_sample_block_);

    void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) override;

    void getTopKMarks(size_t n, std::vector<MinMaxGranule> & result);
    static void getTopKMarks(int direction, size_t n, const std::vector<std::vector<MinMaxGranule>> & parts, std::vector<MarkRanges> & result);

    /// For TopK mode: per-granule values
    std::vector<MinMaxGranule> granules;
    std::unordered_map<size_t, size_t> granules_map;

    /// For Aggregate mode: aggregated min and max across all granules, one Range per column
    std::vector<Range> hyperrectangle;
private:
    Serializations serializations;
    [[maybe_unused]] const String & index_name;
    const Block & index_sample_block;
    FormatSettings format_settings;
    int direction = 0;
    bool empty = true;
    bool store_map = false;
    Mode mode = Mode::TopK;
};

using MergeTreeIndexBulkGranulesMinMaxPtr = std::shared_ptr<MergeTreeIndexBulkGranulesMinMax>;

}
