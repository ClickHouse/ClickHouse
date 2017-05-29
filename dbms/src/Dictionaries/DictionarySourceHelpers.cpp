#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids)
{
    auto column = std::make_shared<ColumnUInt64>(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{ std::move(column), std::make_shared<DataTypeUInt64>(), "id" }};

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

/// For composite key
void formatKeys(const DictionaryStructure & dict_struct, BlockOutputStreamPtr & out,
    const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    Block block;
    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        ColumnPtr filtered_column = source_column->cloneEmpty();
        filtered_column->reserve(requested_rows.size());

        for (size_t idx : requested_rows)
            filtered_column->insertFrom(*source_column, idx);

        block.insert({ filtered_column, (*dict_struct.key)[i].type, toString(i) });
    }

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

}
