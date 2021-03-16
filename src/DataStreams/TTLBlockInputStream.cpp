#include <DataStreams/TTLBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Storages/TTLMode.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


TTLBlockInputStream::TTLBlockInputStream(
    const BlockInputStreamPtr & input_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::MutableDataPartPtr & data_part_,
    time_t current_time_,
    bool force_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , data_part(data_part_)
    , current_time(current_time_)
    , force(force_)
    , old_ttl_infos(data_part->ttl_infos)
    , log(&Poco::Logger::get(storage.getLogName() + " (TTLBlockInputStream)"))
    , date_lut(DateLUT::instance())
{
    children.push_back(input_);
    header = children.at(0)->getHeader();

    const auto & storage_columns = metadata_snapshot->getColumns();
    const auto & column_defaults = storage_columns.getDefaults();

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & [name, _] : metadata_snapshot->getColumnTTLs())
    {
        auto it = column_defaults.find(name);
        if (it != column_defaults.end())
        {
            auto column = storage_columns.get(name);
            auto expression = it->second.expression->clone();
            default_expr_list->children.emplace_back(setAlias(addTypeConversionToAST(std::move(expression), column.type->getName()), it->first));
        }
    }

    for (const auto & [name, ttl_info] : old_ttl_infos.columns_ttl)
    {
        if (force || isTTLExpired(ttl_info.min))
        {
            new_ttl_infos.columns_ttl.emplace(name, IMergeTreeDataPart::TTLInfo{});
            empty_columns.emplace(name);
        }
        else
            new_ttl_infos.columns_ttl.emplace(name, ttl_info);
    }

    if (!force && !isTTLExpired(old_ttl_infos.table_ttl.min))
        new_ttl_infos.table_ttl = old_ttl_infos.table_ttl;

    if (!default_expr_list->children.empty())
    {
        auto syntax_result = TreeRewriter(storage.global_context).analyze(default_expr_list, metadata_snapshot->getColumns().getAllPhysical());
        defaults_expression = ExpressionAnalyzer{default_expr_list, syntax_result, storage.global_context}.getActions(true);
    }

    auto storage_rows_ttl = metadata_snapshot->getRowsTTL();
    if (metadata_snapshot->hasRowsTTL() && storage_rows_ttl.mode == TTLMode::GROUP_BY)
    {
        current_key_value.resize(storage_rows_ttl.group_by_keys.size());

        ColumnNumbers keys;
        for (const auto & key : storage_rows_ttl.group_by_keys)
            keys.push_back(header.getPositionByName(key));
        agg_key_columns.resize(storage_rows_ttl.group_by_keys.size());

        AggregateDescriptions aggregates = storage_rows_ttl.aggregate_descriptions;
        for (auto & descr : aggregates)
            if (descr.arguments.empty())
                for (const auto & name : descr.argument_names)
                    descr.arguments.push_back(header.getPositionByName(name));
        agg_aggregate_columns.resize(storage_rows_ttl.aggregate_descriptions.size());

        const Settings & settings = storage.global_context.getSettingsRef();

        Aggregator::Params params(header, keys, aggregates,
            false, settings.max_rows_to_group_by, settings.group_by_overflow_mode, 0, 0,
            settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
            storage.global_context.getTemporaryVolume(), settings.max_threads, settings.min_free_disk_space_for_temporary_data);
        aggregator = std::make_unique<Aggregator>(params);
    }
}

bool TTLBlockInputStream::isTTLExpired(time_t ttl) const
{
    return (ttl && (ttl <= current_time));
}

Block reorderColumns(Block block, const Block & header)
{
    Block res;
    for (const auto & col : header)
        res.insert(block.getByName(col.name));

    return res;
}

Block TTLBlockInputStream::readImpl()
{
    /// Skip all data if table ttl is expired for part
    auto storage_rows_ttl = metadata_snapshot->getRowsTTL();
    if (metadata_snapshot->hasRowsTTL() && !storage_rows_ttl.where_expression && storage_rows_ttl.mode != TTLMode::GROUP_BY
        && isTTLExpired(old_ttl_infos.table_ttl.max))
    {
        rows_removed = data_part->rows_count;
        return {};
    }


    Block block = children.at(0)->read();
    if (!block)
    {
        if (aggregator && !agg_result.empty())
        {
            MutableColumns result_columns = header.cloneEmptyColumns();
            finalizeAggregates(result_columns);
            block = header.cloneWithColumns(std::move(result_columns));
        }

        return block;
    }

    if (metadata_snapshot->hasRowsTTL() && (force || isTTLExpired(old_ttl_infos.table_ttl.min)))
        removeRowsWithExpiredTableTTL(block);

    removeValuesWithExpiredColumnTTL(block);

    updateMovesTTL(block);
    updateRecompressionTTL(block);

    return reorderColumns(std::move(block), header);
}

void TTLBlockInputStream::readSuffixImpl()
{
    for (const auto & elem : new_ttl_infos.columns_ttl)
        new_ttl_infos.updatePartMinMaxTTL(elem.second.min, elem.second.max);

    new_ttl_infos.updatePartMinMaxTTL(new_ttl_infos.table_ttl.min, new_ttl_infos.table_ttl.max);

    data_part->ttl_infos = std::move(new_ttl_infos);
    data_part->expired_columns = std::move(empty_columns);

    if (rows_removed)
        LOG_DEBUG(log, "Removed {} rows with expired TTL from part {}", rows_removed, data_part->name);
}

void TTLBlockInputStream::removeRowsWithExpiredTableTTL(Block & block)
{
    auto rows_ttl = metadata_snapshot->getRowsTTL();

    rows_ttl.expression->execute(block);
    if (rows_ttl.where_expression)
        rows_ttl.where_expression->execute(block);

    const IColumn * ttl_column =
        block.getByName(rows_ttl.result_column).column.get();

    const IColumn * where_result_column = rows_ttl.where_expression ?
        block.getByName(rows_ttl.where_result_column).column.get() : nullptr;

    const auto & column_names = header.getNames();

    if (!aggregator)
    {
        MutableColumns result_columns;
        result_columns.reserve(column_names.size());
        for (auto it = column_names.begin(); it != column_names.end(); ++it)
        {
            const IColumn * values_column = block.getByName(*it).column.get();
            MutableColumnPtr result_column = values_column->cloneEmpty();
            result_column->reserve(block.rows());

            for (size_t i = 0; i < block.rows(); ++i)
            {
                UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
                bool where_filter_passed = !where_result_column || where_result_column->getBool(i);
                if (!isTTLExpired(cur_ttl) || !where_filter_passed)
                {
                    new_ttl_infos.table_ttl.update(cur_ttl);
                    result_column->insertFrom(*values_column, i);
                }
                else if (it == column_names.begin())
                    ++rows_removed;
            }
            result_columns.emplace_back(std::move(result_column));
        }
        block = header.cloneWithColumns(std::move(result_columns));
    }
    else
    {
        MutableColumns result_columns = header.cloneEmptyColumns();
        MutableColumns aggregate_columns = header.cloneEmptyColumns();

        size_t rows_aggregated = 0;
        size_t current_key_start = 0;
        size_t rows_with_current_key = 0;
        auto storage_rows_ttl = metadata_snapshot->getRowsTTL();
        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            bool where_filter_passed = !where_result_column || where_result_column->getBool(i);
            bool ttl_expired = isTTLExpired(cur_ttl) && where_filter_passed;

            bool same_as_current = true;
            for (size_t j = 0; j < storage_rows_ttl.group_by_keys.size(); ++j)
            {
                const String & key_column = storage_rows_ttl.group_by_keys[j];
                const IColumn * values_column = block.getByName(key_column).column.get();
                if (!same_as_current || (*values_column)[i] != current_key_value[j])
                {
                    values_column->get(i, current_key_value[j]);
                    same_as_current = false;
                }
            }
            if (!same_as_current)
            {
                if (rows_with_current_key)
                    calculateAggregates(aggregate_columns, current_key_start, rows_with_current_key);
                finalizeAggregates(result_columns);

                current_key_start = rows_aggregated;
                rows_with_current_key = 0;
            }

            if (ttl_expired)
            {
                ++rows_with_current_key;
                ++rows_aggregated;
                for (const auto & name : column_names)
                {
                    const IColumn * values_column = block.getByName(name).column.get();
                    auto & column = aggregate_columns[header.getPositionByName(name)];
                    column->insertFrom(*values_column, i);
                }
            }
            else
            {
                new_ttl_infos.table_ttl.update(cur_ttl);
                for (const auto & name : column_names)
                {
                    const IColumn * values_column = block.getByName(name).column.get();
                    auto & column = result_columns[header.getPositionByName(name)];
                    column->insertFrom(*values_column, i);
                }
            }
        }

        if (rows_with_current_key)
            calculateAggregates(aggregate_columns, current_key_start, rows_with_current_key);

        block = header.cloneWithColumns(std::move(result_columns));
    }
}

void TTLBlockInputStream::calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length)
{
    Columns aggregate_chunk;
    aggregate_chunk.reserve(aggregate_columns.size());
    for (const auto & name : header.getNames())
    {
        const auto & column = aggregate_columns[header.getPositionByName(name)];
        ColumnPtr chunk_column = column->cut(start_pos, length);
        aggregate_chunk.emplace_back(std::move(chunk_column));
    }
    aggregator->executeOnBlock(aggregate_chunk, length, agg_result, agg_key_columns,
                               agg_aggregate_columns, agg_no_more_keys);
}

void TTLBlockInputStream::finalizeAggregates(MutableColumns & result_columns)
{
    if (!agg_result.empty())
    {
        auto aggregated_res = aggregator->convertToBlocks(agg_result, true, 1);
        auto storage_rows_ttl = metadata_snapshot->getRowsTTL();
        for (auto & agg_block : aggregated_res)
        {
            for (const auto & it : storage_rows_ttl.set_parts)
                it.expression->execute(agg_block);
            for (const auto & name : storage_rows_ttl.group_by_keys)
            {
                const IColumn * values_column = agg_block.getByName(name).column.get();
                auto & result_column = result_columns[header.getPositionByName(name)];
                result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
            }
            for (const auto & it : storage_rows_ttl.set_parts)
            {
                const IColumn * values_column = agg_block.getByName(it.expression_result_column_name).column.get();
                auto & result_column = result_columns[header.getPositionByName(it.column_name)];
                result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
            }
        }
    }
    agg_result.invalidate();
}

void TTLBlockInputStream::removeValuesWithExpiredColumnTTL(Block & block)
{
    Block block_with_defaults;
    if (defaults_expression)
    {
        block_with_defaults = block;
        defaults_expression->execute(block_with_defaults);
    }

    std::vector<String> columns_to_remove;
    for (const auto & [name, ttl_entry] : metadata_snapshot->getColumnTTLs())
    {
        /// If we read not all table columns. E.g. while mutation.
        if (!block.has(name))
            continue;

        const auto & old_ttl_info = old_ttl_infos.columns_ttl[name];
        auto & new_ttl_info = new_ttl_infos.columns_ttl[name];

        /// Nothing to do
        if (!force && !isTTLExpired(old_ttl_info.min))
            continue;

        /// Later drop full column
        if (isTTLExpired(old_ttl_info.max))
            continue;

        if (!block.has(ttl_entry.result_column))
        {
            columns_to_remove.push_back(ttl_entry.result_column);
            ttl_entry.expression->execute(block);
        }

        ColumnPtr default_column = nullptr;
        if (block_with_defaults.has(name))
            default_column = block_with_defaults.getByName(name).column->convertToFullColumnIfConst();

        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        const IColumn * ttl_column = block.getByName(ttl_entry.result_column).column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            if (isTTLExpired(cur_ttl))
            {
                if (default_column)
                    result_column->insertFrom(*default_column, i);
                else
                    result_column->insertDefault();
            }
            else
            {
                new_ttl_info.update(cur_ttl);
                empty_columns.erase(name);
                result_column->insertFrom(*values_column, i);
            }
        }
        column_with_type.column = std::move(result_column);
    }

    for (const String & column : columns_to_remove)
        block.erase(column);
}

void TTLBlockInputStream::updateTTLWithDescriptions(Block & block, const TTLDescriptions & descriptions, TTLInfoMap & ttl_info_map)
{
    std::vector<String> columns_to_remove;
    for (const auto & ttl_entry : descriptions)
    {
        auto & new_ttl_info = ttl_info_map[ttl_entry.result_column];
        if (!block.has(ttl_entry.result_column))
        {
            columns_to_remove.push_back(ttl_entry.result_column);
            ttl_entry.expression->execute(block);
        }

        const IColumn * ttl_column = block.getByName(ttl_entry.result_column).column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            new_ttl_info.update(cur_ttl);
        }
    }

    for (const String & column : columns_to_remove)
        block.erase(column);
}

void TTLBlockInputStream::updateMovesTTL(Block & block)
{
    updateTTLWithDescriptions(block, metadata_snapshot->getMoveTTLs(), new_ttl_infos.moves_ttl);
}

void TTLBlockInputStream::updateRecompressionTTL(Block & block)
{
    updateTTLWithDescriptions(block, metadata_snapshot->getRecompressionTTLs(), new_ttl_infos.recompression_ttl);
}

UInt32 TTLBlockInputStream::getTimestampByIndex(const IColumn * column, size_t ind)
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[ind]));
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[ind];
    else if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(column))
    {
        if (typeid_cast<const ColumnUInt16 *>(&column_const->getDataColumn()))
            return date_lut.fromDayNum(DayNum(column_const->getValue<UInt16>()));
        else if (typeid_cast<const ColumnUInt32 *>(&column_const->getDataColumn()))
            return column_const->getValue<UInt32>();
    }

    throw Exception("Unexpected type of result TTL column", ErrorCodes::LOGICAL_ERROR);
}

}
