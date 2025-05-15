#include "LanceSource.h"

#if USE_LANCE

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

LanceSource::LanceSource(LanceTableReader reader_, const Block & sample_block)
    : ISource(sample_block.cloneEmpty()), reader(std::move(reader_))
{
    description.init(sample_block);
}

Chunk LanceSource::generate()
{
    MutableColumns columns = description.sample_block.cloneEmptyColumns();

    if (!reader.readNextBatch())
    {
        return {};
    }

    size_t num_rows = 0;

    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & [type, is_nullable] = description.types[i];
        const String & name = description.sample_block.getByPosition(i).name;
        size_t nrows = insertColumn(*columns[i], type, name, is_nullable);
        if (num_rows == 0)
        {
            num_rows = nrows;
        }
        else if (num_rows != nrows)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of rows in Lance table mismatch in different columns");
        }
    }

    return Chunk(std::move(columns), num_rows);
}

size_t LanceSource::insertColumn(IColumn & column, ExternalResultDescription::ValueType type, const String & column_name, bool is_nullable)
{
    if (is_nullable && reader.getNulls(column_name).empty())
    {
        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(column);
        size_t nrows = insertColumn(column_nullable.getNestedColumn(), type, column_name, false);
        for (size_t i = 0; i < nrows; ++i)
        {
            column_nullable.getNullMapData().emplace_back(0);
        }
        return nrows;
    }
    switch (type)
    {
        case ExternalResultDescription::ValueType::vtInt8: {
            return insertNumericColumn<ColumnInt8>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtUInt8: {
            return insertNumericColumn<ColumnUInt8>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtInt16: {
            return insertNumericColumn<ColumnInt16>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtUInt16: {
            return insertNumericColumn<ColumnUInt16>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtInt32: {
            return insertNumericColumn<ColumnInt32>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtUInt32: {
            return insertNumericColumn<ColumnUInt32>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtInt64: {
            return insertNumericColumn<ColumnInt64>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtUInt64: {
            return insertNumericColumn<ColumnUInt64>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtFloat32: {
            return insertNumericColumn<ColumnFloat32>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtFloat64: {
            return insertNumericColumn<ColumnFloat64>(column, column_name, is_nullable);
        }
        case ExternalResultDescription::ValueType::vtString: {
            auto lance_column = reader.getColumnFromCurrentBatch<const char *>(column_name, is_nullable);
            if (!is_nullable)
            {
                for (auto & value : lance_column)
                {
                    assert_cast<ColumnString &>(column).insertData(value, std::strlen(value));
                }
                return lance_column.size();
            }
            ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(column);
            ColumnString & nested_column = assert_cast<ColumnString &>(column_nullable.getNestedColumn());
            auto nulls = reader.getNulls(column_name);
            for (size_t i = 0; i < lance_column.size(); ++i)
            {
                if (nulls[i])
                {
                    column_nullable.insertDefault();
                }
                else
                {
                    nested_column.insertData(lance_column[i], std::strlen(lance_column[i]));
                    column_nullable.getNullMapData().emplace_back(0);
                }
            }
            return lance_column.size();
        }
        default: {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupproted type for Lance engine: {}", type);
        }
    }
}

} // namespace DB

#endif
