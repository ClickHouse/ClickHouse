#include "LanceSink.h"

#if USE_LANCE

#    include "LanceUtils.h"

#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

LanceSink::LanceSink(const StorageMetadataPtr & metadata_snapshot_, LanceTablePtr lance_table_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock()), lance_table(std::move(lance_table_))
{
}

void LanceSink::consume(Chunk & chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    std::vector<lance::ColumnDescription> schema_vector;

    lance::Batch * batch = lance::create_batch();

    for (auto & column_with_type_and_name : block)
    {
        DataTypePtr data_type = column_with_type_and_name.type;
        bool is_nullable = false;
        if (data_type->getColumnType() == TypeIndex::Nullable)
        {
            const IDataType & idata_type = *column_with_type_and_name.type;
            const DataTypeNullable & nullable_data_type = assert_cast<const DataTypeNullable &>(idata_type);
            data_type = nullable_data_type.getNestedType();
            is_nullable = true;
        }
        schema_vector.emplace_back(
            column_with_type_and_name.name.c_str(), typeIndexToLanceColumnType(data_type->getColumnType()), is_nullable);

        appendColumn(column_with_type_and_name.column, data_type, batch, is_nullable);
    }


    lance::Schema schema{.data = schema_vector.data(), .len = schema_vector.size(), .capacity = schema_vector.capacity()};
    lance::set_schema_for_batch(batch, schema);

    lance::write_batch_to_table(lance_table->table, batch);
    lance::free_batch(batch);
}

void LanceSink::appendColumn(ColumnPtr column, DataTypePtr data_type, lance::Batch * batch, bool is_nullable)
{
    switch (data_type->getColumnType())
    {
        case TypeIndex::Int8: {
            appendNumericColumn<Int8>(column, lance::ColumnType::Int8, batch, is_nullable);
            return;
        }
        case TypeIndex::UInt8: {
            appendNumericColumn<UInt8>(column, lance::ColumnType::UInt8, batch, is_nullable);
            return;
        }
        case TypeIndex::Int16: {
            appendNumericColumn<Int16>(column, lance::ColumnType::Int16, batch, is_nullable);
            return;
        }
        case TypeIndex::UInt16: {
            appendNumericColumn<UInt16>(column, lance::ColumnType::UInt16, batch, is_nullable);
            return;
        }
        case TypeIndex::Int32: {
            appendNumericColumn<Int32>(column, lance::ColumnType::Int32, batch, is_nullable);
            return;
        }
        case TypeIndex::UInt32: {
            appendNumericColumn<UInt32>(column, lance::ColumnType::UInt32, batch, is_nullable);
            return;
        }
        case TypeIndex::Int64: {
            appendNumericColumn<Int64>(column, lance::ColumnType::Int64, batch, is_nullable);
            return;
        }
        case TypeIndex::UInt64: {
            appendNumericColumn<UInt64>(column, lance::ColumnType::UInt64, batch, is_nullable);
            return;
        }
        case TypeIndex::Float32: {
            appendNumericColumn<Float32>(column, lance::ColumnType::Float32, batch, is_nullable);
            return;
        }
        case TypeIndex::Float64: {
            appendNumericColumn<Float64>(column, lance::ColumnType::Float64, batch, is_nullable);
            return;
        }
        case TypeIndex::String: {
            std::vector<const char *> values(column->size());
            if (!is_nullable)
            {
                for (size_t i = 0; i < column->size(); ++i)
                {
                    values[i] = assert_cast<const ColumnString &>(*column).getDataAt(i).data;
                }
                lance::Column lance_column{
                    .data_type = lance::ColumnType::String,
                    .data = values.data(),
                    .len = values.size(),
                    .capacity = values.capacity(),
                };
                lance::append_column_to_batch(batch, lance_column);
                return;
            }
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(*column);
            const IColumn & nested_column = column_nullable.getNestedColumn();
            std::vector<UInt8> nulls(column->size());
            bool any_nulls = false;
            for (size_t i = 0; i < column->size(); ++i)
            {
                nulls[i] = column_nullable.isNullAt(i);
                if (nulls[i])
                {
                    values[i] = nullptr;
                    any_nulls = true;
                }
                else
                {
                    values[i] = assert_cast<const ColumnString &>(nested_column).getDataAt(i).data;
                }
            }

            lance::Column lance_column{
                .data_type = lance::ColumnType::String,
                .data = values.data(),
                .len = values.size(),
                .capacity = values.capacity(),
            };
            if (!any_nulls)
            {
                lance::append_column_to_batch(batch, lance_column);
            }
            else
            {
                lance::NullableColumn nullable_lance_column{.column = lance_column, .nulls = reinterpret_cast<bool *>(nulls.data())};
                lance::append_nullable_column_to_batch(batch, nullable_lance_column);
            }
            return;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type for Lance engine: {}", data_type->getColumnType());
    }
}

} // namespace DB

#endif
