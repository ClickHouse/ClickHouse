#include <Processors/Transforms/TTLDeleteFilterTransform.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static TTLExpressions buildTTLExpressions(
    const TTLDescription & ttl_descr,
    PreparedSets::Subqueries & subqueries_for_sets,
    const ContextPtr & context)
{
    auto expr = ttl_descr.buildExpression(context);
    auto expr_queries = expr.sets->getSubqueries();
    subqueries_for_sets.insert(subqueries_for_sets.end(), expr_queries.begin(), expr_queries.end());

    auto where_expr = ttl_descr.buildWhereExpression(context);
    if (where_expr.sets)
    {
        auto where_expr_queries = where_expr.sets->getSubqueries();
        subqueries_for_sets.insert(subqueries_for_sets.end(), where_expr_queries.begin(), where_expr_queries.end());
    }

    return {expr.expression, where_expr.expression};
}

SharedHeader TTLDeleteFilterTransform::transformHeader(const SharedHeader & header)
{
    auto result = *header;
    result.insert({std::make_shared<DataTypeUInt8>()->createColumn(), std::make_shared<DataTypeUInt8>(), TTL_FILTER_COLUMN_NAME});
    return std::make_shared<const Block>(std::move(result));
}

TTLDeleteFilterTransform::TTLDeleteFilterTransform(
    const ContextPtr & context,
    const SharedHeader & header_,
    const StorageMetadataPtr & metadata_snapshot_,
    const IMergeTreeDataPart::TTLInfos & old_ttl_infos_,
    time_t current_time_,
    bool force_,
    const MergeTreeMutableDataPartPtr & data_part_)
    : ISimpleTransform(header_, transformHeader(header_), /*skip_empty_chunks=*/ false)
    , current_time(current_time_)
    , force(force_)
    , date_lut(DateLUT::instance())
    , data_part(data_part_)
{
    if (metadata_snapshot_->hasRowsTTL())
    {
        const auto & rows_ttl = metadata_snapshot_->getRowsTTL();
        auto expressions = buildTTLExpressions(rows_ttl, subqueries_for_sets, context);

        DeleteTTLEntry entry;
        entry.expressions = std::move(expressions);
        entry.description = rows_ttl;
        entry.old_ttl_info = old_ttl_infos_.table_ttl;

        if (!isMinTTLExpired(entry.old_ttl_info))
            entry.new_ttl_info = entry.old_ttl_info;
        if (isTTLExpired(entry.old_ttl_info.max))
            entry.new_ttl_info.ttl_finished = true;

        if (isMinTTLExpired(entry.old_ttl_info)
            && isTTLExpired(entry.old_ttl_info.max)
            && !rows_ttl.where_expression_ast)
        {
            all_data_dropped = true;
        }

        delete_ttl_entries.emplace_back(std::move(entry));
    }

    for (const auto & where_ttl : metadata_snapshot_->getRowsWhereTTLs())
    {
        auto expressions = buildTTLExpressions(where_ttl, subqueries_for_sets, context);

        DeleteTTLEntry entry;
        entry.expressions = std::move(expressions);
        entry.description = where_ttl;
        auto it = old_ttl_infos_.rows_where_ttl.find(where_ttl.result_column);
        if (it != old_ttl_infos_.rows_where_ttl.end())
            entry.old_ttl_info = it->second;

        if (!isMinTTLExpired(entry.old_ttl_info))
            entry.new_ttl_info = entry.old_ttl_info;
        if (isTTLExpired(entry.old_ttl_info.max))
            entry.new_ttl_info.ttl_finished = true;

        delete_ttl_entries.emplace_back(std::move(entry));
    }
}

bool TTLDeleteFilterTransform::isTTLExpired(time_t ttl) const
{
    return ttl && (ttl <= current_time);
}

bool TTLDeleteFilterTransform::isMinTTLExpired(const IMergeTreeDataPart::TTLInfo & info) const
{
    return force || isTTLExpired(info.min);
}

void TTLDeleteFilterTransform::extractTimestamps(const IColumn * ttl_column, size_t num_rows)
{
    timestamps.resize_exact(num_rows);

    if (const auto * col_date = typeid_cast<const ColumnUInt16 *>(ttl_column))
    {
        const auto & data = col_date->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = date_lut.fromDayNum(DayNum(data[i]));
    }
    else if (const auto * col_datetime = typeid_cast<const ColumnUInt32 *>(ttl_column))
    {
        const auto & data = col_datetime->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = static_cast<Int64>(data[i]);
    }
    else if (const auto * col_date32 = typeid_cast<const ColumnInt32 *>(ttl_column))
    {
        const auto & data = col_date32->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = date_lut.fromDayNum(ExtendedDayNum(data[i]));
    }
    else if (const auto * col_datetime64 = typeid_cast<const ColumnDateTime64 *>(ttl_column))
    {
        const auto & data = col_datetime64->getData();
        const auto scale = intExp10OfSize<Int64>(col_datetime64->getScale());
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = data[i] / scale;
    }
    else if (const auto * col_const = typeid_cast<const ColumnConst *>(ttl_column))
    {
        /// Same inner-type dispatch as ITTLAlgorithm::getTimestampByIndex,
        /// but only executed once for the constant value.
        const auto & inner = col_const->getDataColumn();
        Int64 value;
        if (typeid_cast<const ColumnUInt16 *>(&inner))
            value = date_lut.fromDayNum(DayNum(col_const->getValue<UInt16>()));
        else if (typeid_cast<const ColumnUInt32 *>(&inner))
            value = col_const->getValue<UInt32>();
        else if (typeid_cast<const ColumnInt32 *>(&inner))
            value = date_lut.fromDayNum(ExtendedDayNum(col_const->getValue<Int32>()));
        else if (const auto * inner_dt64 = typeid_cast<const ColumnDateTime64 *>(&inner))
            value = col_const->getValue<DateTime64>() / intExp10OfSize<Int64>(inner_dt64->getScale());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of inner column in constant TTL column");

        std::fill(timestamps.begin(), timestamps.end(), value);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of result TTL column");
    }
}

void TTLDeleteFilterTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();

    if (all_data_dropped)
    {
        chunk.addColumn(ColumnUInt8::create(num_rows, UInt8(0)));
        return;
    }

    if (num_rows == 0)
    {
        chunk.addColumn(ColumnUInt8::create());
        return;
    }

    auto filter_data = ColumnUInt8::create(num_rows, UInt8(1));
    auto & filter_vec = filter_data->getData();

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    for (auto & entry : delete_ttl_entries)
    {
        if (!isMinTTLExpired(entry.old_ttl_info))
            continue;

        /// Phase 1: extract typed TTL column into a flat Int64 timestamp array.
        auto ttl_column = ITTLAlgorithm::executeExpressionAndGetColumn(
            entry.expressions.expression, block, entry.description.result_column);
        extractTimestamps(ttl_column.get(), num_rows);

        /// Phase 2: apply TTL expiration and WHERE filter to produce the filter mask.
        auto where_column = ITTLAlgorithm::executeExpressionAndGetColumn(
            entry.expressions.where_expression, block, entry.description.where_result_column);

        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!filter_vec[i])
                continue;

            bool where_filter_passed = !where_column || where_column->getBool(i);
            if (isTTLExpired(timestamps[i]) && where_filter_passed)
            {
                filter_vec[i] = 0;
            }
            else if (where_filter_passed)
            {
                entry.new_ttl_info.update(timestamps[i]);
            }
        }
    }

    chunk = Chunk(block.getColumns(), num_rows);
    chunk.addColumn(std::move(filter_data));
}

void TTLDeleteFilterTransform::finalize()
{
    if (finalized)
        return;
    finalized = true;

    for (const auto & entry : delete_ttl_entries)
    {
        if (entry.expressions.where_expression)
            data_part->ttl_infos.rows_where_ttl[entry.description.result_column] = entry.new_ttl_info;
        else
            data_part->ttl_infos.table_ttl = entry.new_ttl_info;

        data_part->ttl_infos.updatePartMinMaxTTL(entry.new_ttl_info.min, entry.new_ttl_info.max);
    }
}

IProcessor::Status TTLDeleteFilterTransform::prepare()
{
    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
        finalize();
    return status;
}

}
