#include <DataStreams/TTLBlockInputStream.h>

namespace DB
{

TTLBlockInputStream::TTLBlockInputStream(
    const BlockInputStreamPtr & input_,
    const MergeTreeData & storage_,
    const MergeTreeData::MutableDataPartPtr & data_part_,
    time_t current_time_)
    : storage(storage_)
    , data_part(data_part_)
    , current_time(current_time_)
{
    children.push_back(input_);

    for (const auto & elem : storage.ttl_entries_by_name)
        data_part->empty_columns.emplace(elem.first);
}


Block TTLBlockInputStream::getHeader() const
{
    return children.at(0)->getHeader();
}

Block TTLBlockInputStream::readImpl()
{
    Block block = children.at(0)->read();
    if (!block)
        return block;

    if (storage.hasTableTTL())
        removeRowsWithExpiredTableTTL(block);

    removeValuesWithExpiredColumnTTL(block);

    return block;
}

void TTLBlockInputStream::removeRowsWithExpiredTableTTL(Block & block)
{
    storage.ttl_table_entry.expression->execute(block);

    const auto & ttl_result_column = block.getByName(storage.ttl_table_entry.result_column);
    const ColumnUInt32 * ttl_table_result_column = typeid_cast<const ColumnUInt32 *>(ttl_result_column.column.get());

    MutableColumns result_columns;
    result_columns.reserve(getHeader().columns());
    for (const auto & name : storage.getColumns().getNamesOfPhysical())
    {
        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            time_t cur_ttl = ttl_table_result_column->getData()[i];
            if (cur_ttl > current_time)
            {
                if (!data_part->min_ttl || cur_ttl < data_part->min_ttl)
                    data_part->min_ttl = cur_ttl;

                result_column->insertFrom(*values_column, i);
            }
        }
        result_columns.emplace_back(std::move(result_column));
    }
    block = getHeader().cloneWithColumns(std::move(result_columns));
}

void TTLBlockInputStream::removeValuesWithExpiredColumnTTL(Block & block)
{
    for (const auto & [name, ttl_entry] : storage.ttl_entries_by_name)
    {
        if (!block.has(ttl_entry.result_column))
            ttl_entry.expression->execute(block);

        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        const auto & ttl_result_column = block.getByName(ttl_entry.result_column);

        const ColumnUInt32::Container & ttl_vec =
            (typeid_cast<const ColumnUInt32 *>(ttl_result_column.column.get()))->getData();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (ttl_vec[i] <= current_time)
                result_column->insertDefault();
            else
            {
                if (!data_part->min_ttl || ttl_vec[i] < data_part->min_ttl)
                    data_part->min_ttl = ttl_vec[i];

                data_part->empty_columns.erase(name);
                result_column->insertFrom(*values_column, i);
            }
        }
        column_with_type.column = std::move(result_column);
    }

    for (const auto & elem : storage.ttl_entries_by_name)
        if (block.has(elem.second.result_column))
            block.erase(elem.second.result_column);
}

}
