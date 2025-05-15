#pragma once

#include "config.h"

#if USE_LANCE

#    include "LanceTable.h"

#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnVector.h>
#    include <Processors/Sinks/SinkToStorage.h>
#    include <Storages/StorageInMemoryMetadata.h>

#    include <lance.h>

namespace DB
{

class LanceSink : public SinkToStorage
{
public:
    LanceSink(const StorageMetadataPtr & metadata_snapshot_, LanceTablePtr lance_table_);

    String getName() const override { return "LanceSink"; }

    void consume(Chunk & chunk) override;

private:
    void appendColumn(ColumnPtr column, DataTypePtr data_type, lance::Batch * batch, bool is_nullable);

    template <typename T>
    void appendNumericColumn(ColumnPtr column, lance::ColumnType column_type, lance::Batch * batch, bool is_nullable)
    {
        if (!is_nullable)
        {
            std::vector<T> values(column->size());
            for (size_t i = 0; i < column->size(); ++i)
            {
                values[i] = assert_cast<const ColumnVector<T> &>(*column).getElement(i);
            }

            lance::Column lance_column{
                .data_type = column_type,
                .data = values.data(),
                .len = values.size(),
                .capacity = values.capacity(),
            };
            lance::append_column_to_batch(batch, lance_column);
        }
        else
        {
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(*column);
            const IColumn & nested_column = column_nullable.getNestedColumn();
            std::vector<T> values(column->size());
            std::vector<UInt8> nulls(column->size());
            bool any_nulls = false;
            for (size_t i = 0; i < column->size(); ++i)
            {
                nulls[i] = column_nullable.isNullAt(i);
                if (nulls[i])
                {
                    values[i] = 0;
                    any_nulls = true;
                }
                else
                {
                    values[i] = assert_cast<const ColumnVector<T> &>(nested_column).getElement(i);
                }
            }

            lance::Column lance_column{
                .data_type = column_type,
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
        }
    }

private:
    LanceTablePtr lance_table;
};

} // namespace DB

#endif
