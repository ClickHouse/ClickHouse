#include <Storages/MergeTree/MergeTreeIndexRowBitmap.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleRowBitmap::MergeTreeIndexGranuleRowBitmap(const Block & index_sample_block_)
    : block(index_sample_block_.cloneEmpty())
{
    /// Keep one serialization object per stored scalar expression column so index files can use the
    /// same binary encoding as ordinary ClickHouse columns.
    serializations.reserve(block.columns());
    for (const auto & column : block)
        serializations.push_back(column.type->getDefaultSerialization());
}

MergeTreeIndexGranuleRowBitmap::MergeTreeIndexGranuleRowBitmap(Block && block_)
    : block(std::move(block_))
{
    /// The deserialized or aggregator-produced block already contains rows; initialize matching
    /// serializations for later reads/writes of this index granule.
    serializations.reserve(block.columns());
    for (const auto & column : block)
        serializations.push_back(column.type->getDefaultSerialization());
}

void MergeTreeIndexGranuleRowBitmap::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty row_bitmap index granule");

    /// Persist the number of rows first because the bitmap itself is built at query time. The stored
    /// payload remains the scalar values needed to re-evaluate different WHERE predicates later.
    const UInt64 rows_to_write = block.rows();
    writeVarUInt(rows_to_write, ostr);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = *block.getByPosition(i).column;

        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
        settings.position_independent_encoding = false;
        settings.low_cardinality_max_dictionary_size = 0;

        ISerialization::SerializeBinaryBulkStatePtr state;
        serializations[i]->serializeBinaryBulkStatePrefix(column, settings, state);
        serializations[i]->serializeBinaryBulkWithMultipleStreams(column, 0, rows_to_write, settings, state);
        serializations[i]->serializeBinaryBulkStateSuffix(settings, state);
    }
}

void MergeTreeIndexGranuleRowBitmap::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown row_bitmap index version {}", version);

    UInt64 rows_to_read = 0;
    readVarUInt(rows_to_read, istr);

    /// Reconstruct the scalar-value block exactly as it was written. Row order must be preserved,
    /// because bit positions are interpreted as physical row offsets inside the index granule.
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & elem = block.getByPosition(i);
        elem.column = elem.column->cloneEmpty();

        ISerialization::DeserializeBinaryBulkStatePtr state;
        serializations[i]->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        serializations[i]->deserializeBinaryBulkWithMultipleStreams(elem.column, 0, rows_to_read, settings, state, nullptr);
    }
}

MergeTreeIndexAggregatorRowBitmap::MergeTreeIndexAggregatorRowBitmap(const Block & index_sample_block_)
    : block(index_sample_block_.cloneEmpty())
{
    /// Start with an empty row-aligned block; update() appends the scalar values covered by each
    /// index granule as MergeTree writes or materializes the skip index.
}

void MergeTreeIndexAggregatorRowBitmap::update(const Block & block_, size_t * pos, size_t limit)
{
    if (*pos >= block_.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, block rows: {}",
            *pos,
            block_.rows());

    const size_t rows_read = std::min(limit, block_.rows() - *pos);
    if (rows_read == 0)
        return;

    /// Copy indexed expression columns without aggregation. Unlike minmax/set/bloom filters, this
    /// index intentionally keeps row-level scalar values so query-time filtering can be exact.
    for (auto & destination : block)
    {
        const auto & source = block_.getByName(destination.name).column;
        auto mutable_column = IColumn::mutate(std::move(destination.column));
        mutable_column->insertRangeFrom(*source, *pos, rows_read);
        destination.column = std::move(mutable_column);
    }

    *pos += rows_read;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorRowBitmap::getGranuleAndReset()
{
    /// Hand off the accumulated row-aligned scalar block as one index granule, then reset the
    /// aggregator for the next granule built by the MergeTree index writer.
    MutableColumns columns = block.mutateColumns();
    Block granule_block = block.cloneWithColumns(std::move(columns));
    block = granule_block.cloneEmpty();
    return std::make_shared<MergeTreeIndexGranuleRowBitmap>(std::move(granule_block));
}

MergeTreeIndexConditionRowBitmap::MergeTreeIndexConditionRowBitmap(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & index_sample_block)
{
    /// A row_bitmap index can only produce an exact ANN row filter when the whole predicate can be
    /// evaluated from columns stored in the index expression. Partial predicate extraction would make
    /// the bitmap only a coarse prefilter and would reintroduce top-N underfill after FilterStep.
    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(
        predicate,
        &index_sample_block,
        context,
        /*allow_partial_result=*/ false);
    if (!dag)
        return;

    actions_output_column_name = dag->getOutputs().front()->result_name;
    actions = std::make_shared<ExpressionActions>(std::move(*dag), ExpressionActionsSettings(context));
}

VectorSearchFilter MergeTreeIndexConditionRowBitmap::buildFilter(const MergeTreeIndexGranuleRowBitmap & granule) const
{
    /// The granule block contains the indexed scalar expression values for every physical row in
    /// this row_bitmap granule. Execute the extracted WHERE DAG directly on that block, producing one
    /// UInt8 filter value per stored row without reading the base data columns.
    Block block_to_filter = granule.block;
    actions->execute(block_to_filter);

    ColumnPtr filter_column = FilterDescription::preprocessFilterColumn(block_to_filter.getByName(actions_output_column_name).column);
    const auto & filter_data = assert_cast<const ColumnUInt8 &>(*filter_column).getData();

    /// Convert ClickHouse's byte-per-row filter into a compact bitset. The bit position is the row
    /// offset local to this row_bitmap granule; MergeTreeDataSelectExecutor later remaps it to the
    /// vector index granule's local row offsets before exact filtered vector search.
    std::vector<UInt64> bitmap_words((filter_data.size() + 63) / 64, 0);
    UInt64 set_bits = 0;
    for (size_t row = 0; row < filter_data.size(); ++row)
    {
        if (!filter_data[row])
            continue;

        bitmap_words[row / 64] |= 1ULL << (row % 64);
        ++set_bits;
    }

    VectorSearchFilter filter;
    /// Store a positive WHERE bitmap: set bits are rows that may enter vector-search candidates.
    filter.setDenseBitmap(
        std::move(bitmap_words),
        filter_data.size(),
        set_bits);
    return filter;
}

bool MergeTreeIndexConditionRowBitmap::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr granule,
    const UpdatePartialDisjunctionResultFn & /*update_partial_disjunction_result_fn*/) const
{
    if (!actions)
        return true;

    /// For ordinary skip-index pruning, a row_bitmap granule can be skipped only if the exact row
    /// bitmap contains no allowed rows. This reuses the same bitmap logic consumed by vector search.
    const auto & row_bitmap_granule = assert_cast<const MergeTreeIndexGranuleRowBitmap &>(*granule);
    return !buildFilter(row_bitmap_granule).noAllowedRows();
}

std::optional<VectorSearchFilter> MergeTreeIndexConditionRowBitmap::calculateVectorSearchFilter(MergeTreeIndexGranulePtr granule) const
{
    if (!actions)
        return std::nullopt;

    /// Return the exact row predicate to MergeTreeDataSelectExecutor. Non-row-bitmap indexes keep the
    /// default nullopt implementation, so only exact row-level providers feed filtered vector search.
    const auto & row_bitmap_granule = assert_cast<const MergeTreeIndexGranuleRowBitmap &>(*granule);
    return buildFilter(row_bitmap_granule);
}

MergeTreeIndexGranulePtr MergeTreeIndexRowBitmap::createIndexGranule() const
{
    /// Readers fill this granule by deserializing persisted scalar values from the row_bitmap file.
    return std::make_shared<MergeTreeIndexGranuleRowBitmap>(index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexRowBitmap::createIndexAggregator() const
{
    /// Writers use the aggregator to collect all scalar expression values for the current granule.
    return std::make_shared<MergeTreeIndexAggregatorRowBitmap>(index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexRowBitmap::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    /// Build the condition against the index sample block, not the storage sample block, to enforce
    /// that pushed row filters are computed solely from columns persisted in this row_bitmap index.
    return std::make_shared<MergeTreeIndexConditionRowBitmap>(predicate, context, index.sample_block);
}

MergeTreeIndexPtr rowBitmapIndexCreator(
    StorageMetadataPtr metadata_snapshot,
    const IndexDescription & index,
    const MergeTreeSettings &)
{
    /// Factory entry point used by CREATE TABLE / ATTACH TABLE when the DDL references TYPE row_bitmap.
    return std::make_shared<MergeTreeIndexRowBitmap>(std::move(metadata_snapshot), index);
}

void rowBitmapIndexValidator(const IndexDescription & index, bool, const MergeTreeSettings &)
{
    /// Keep the first version minimal and deterministic: no index-level tuning arguments are accepted.
    if (index.arguments && !index.arguments->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "row_bitmap index does not accept arguments");
}

}
