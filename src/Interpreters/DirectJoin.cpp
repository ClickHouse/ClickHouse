#include <Interpreters/DirectJoin.h>

#include <Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_JOIN_KEYS;
}

void DirectKeyValueJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> &)
{
    if (!table_join->oneDisjunct()
        || table_join->getOnlyClause().key_names_left.size() != 1
        || table_join->getOnlyClause().key_names_right.size() != 1)
    {
        throw DB::Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Not supported by direct JOIN");
    }

    const auto & key_names_left = table_join->getOnlyClause().key_names_left;

    const String & key_name = key_names_left[0];
    const ColumnWithTypeAndName & key_col = block.getByName(key_name);
    if (!key_col.column)
        return;

    NullMap null_map(key_col.column->size(), 1);
    Chunk joined_chunk = storage->getByKeys(key_col, right_sample_block, &null_map);

    Columns cols = joined_chunk.detachColumns();
    for (size_t i = 0; i < cols.size(); ++i)
    {
        ColumnWithTypeAndName col = right_sample_block.getByPosition(i);
        col.column = std::move(cols[i]);
        block.insert(std::move(col));
    }

    if (!isLeftOrFull(table_join->kind()))
    {
        MutableColumns dst_columns = block.mutateColumns();

        for (auto & col : dst_columns)
        {
            col = IColumn::mutate(col->filter(null_map, -1));
        }
        block.setColumns(std::move(dst_columns));
    }
}

}
