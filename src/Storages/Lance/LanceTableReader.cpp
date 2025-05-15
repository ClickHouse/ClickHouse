#include "LanceTableReader.h"

#if USE_LANCE

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

LanceTableReader::LanceTableReader(LanceTablePtr table_, size_t max_rows_in_block_)
    : table(std::move(table_))
    , reader(lance::create_reader(table->table))
    , columns_in_batch(table->getSchema().size(), lance::Column{lance::ColumnType::Unknown, nullptr, 0, 0})
    , nulls(table->getSchema().size(), nullptr)
    , max_rows_in_block(max_rows_in_block_)
{
}

LanceTableReader::LanceTableReader(LanceTableReader && reader_)
    : table(std::move(reader_.table))
    , reader(std::exchange(reader_.reader, nullptr))
    , columns_in_batch(std::move(reader_.columns_in_batch))
    , nulls(std::move(reader_.nulls))
    , max_rows_in_block(reader_.max_rows_in_block)
{
}

bool LanceTableReader::readNextBatch()
{
    if (rows_end != rows_in_batch)
    {
        rows_begin = rows_end;
        rows_end += max_rows_in_block;
        if (rows_end > rows_in_batch)
        {
            rows_end = rows_in_batch;
        }
        return true;
    }

    FreeColumns();
    if (lance::read_next_batch(reader))
    {
        rows_in_batch = lance::get_rows_in_current_batch(reader);
        rows_begin = 0;
        rows_end = max_rows_in_block;
        if (rows_end > rows_in_batch)
        {
            rows_end = rows_in_batch;
        }
        return true;
    }
    return false;
}

std::span<bool> LanceTableReader::getNulls(const String & name)
{
    bool * nulls_ptr = getNullsFromColumn(name);
    if (nulls_ptr == nullptr)
    {
        return {};
    }
    return {nulls_ptr + rows_begin, nulls_ptr + rows_end};
}

LanceTableReader::~LanceTableReader()
{
    if (reader != nullptr)
    {
        lance::free_reader(reader);
    }
}

lance::Column & LanceTableReader::getColumn(const String & name, bool nullable)
{
    size_t idx = table->getColumnIndexByName(name);
    lance::Column & column = columns_in_batch[idx];
    if (column.data == nullptr)
    {
        if (nullable)
        {
            lance::NullableColumn nullable_column = lance::get_nullable_column(reader, idx);
            nulls[idx] = nullable_column.nulls;
            column = nullable_column.column;
        }
        else
        {
            column = lance::get_column(reader, idx);
        }
    }
    return column;
}

bool * LanceTableReader::getNullsFromColumn(const String & name)
{
    size_t idx = table->getColumnIndexByName(name);
    lance::Column & column = columns_in_batch[idx];
    if (column.data == nullptr)
    {
        getColumn(name, true);
    }
    return nulls[idx];
}

void LanceTableReader::FreeColumns()
{
    for (size_t i = 0; i < columns_in_batch.size(); ++i)
    {
        if (columns_in_batch[i].data != nullptr)
        {
            if (nulls[i] != nullptr)
            {
                lance::free_nullable_column(lance::NullableColumn{.column = columns_in_batch[i], .nulls = nulls[i]});
                nulls[i] = nullptr;
            }
            else
            {
                lance::free_column(columns_in_batch[i]);
            }
        }
        columns_in_batch[i].data = nullptr;
    }
}

} // namespace DB

#endif
