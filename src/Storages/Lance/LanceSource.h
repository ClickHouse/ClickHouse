#pragma once

#include "config.h"

#if USE_LANCE

#    include "LanceTableReader.h"

#    include <Columns/ColumnNullable.h>
#    include <Core/ExternalResultDescription.h>
#    include <Processors/ISource.h>

namespace DB
{

class LanceSource final : public ISource
{
public:
    LanceSource(LanceTableReader reader_, const Block & sample_block);

    String getName() const override { return "Lance"; }

private:
    Chunk generate() override;

    size_t insertColumn(IColumn & column, ExternalResultDescription::ValueType type, const String & column_name, bool is_nullable);

    template <typename ColumnType, typename DataType = ColumnType::ValueType>
    size_t insertNumericColumn(IColumn & column, const String & column_name, bool is_nullable)
    {
        auto clmn = reader.getColumnFromCurrentBatch<DataType>(column_name, is_nullable);
        if (!is_nullable)
        {
            for (auto & value : clmn)
            {
                assert_cast<ColumnType &>(column).insertValue(value);
            }
            return clmn.size();
        }
        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(column);
        ColumnType & nested_column = assert_cast<ColumnType &>(column_nullable.getNestedColumn());
        auto nulls = reader.getNulls(column_name);
        for (size_t i = 0; i < clmn.size(); ++i)
        {
            if (nulls[i])
            {
                column_nullable.insertDefault();
            }
            else
            {
                nested_column.insertValue(clmn[i]);
                column_nullable.getNullMapData().emplace_back(0);
            }
        }
        return clmn.size();
    }

private:
    LanceTableReader reader;
    ExternalResultDescription description;
};

} // namespace DB

#endif
