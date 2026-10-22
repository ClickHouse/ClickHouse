#pragma once

#include <Interpreters/IKeyValueEntity.h>

#include <Common/Arena.h>
#include <Common/ArenaAllocator.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Core/Block.h>


namespace DB
{

class MergeTreeTableJoinEntity final : public IKeyValueEntity
{
public:
    using IndexPositions = PODArray<UInt64, 4, AlignedArenaAllocator<sizeof(UInt64)>>;
    using KeyToRowsMap = HashMap<UInt128, IndexPositions, UInt128TrivialHash>;

    MergeTreeTableJoinEntity(Names primary_key_, Block data_block_);

    Names getPrimaryKey() const override;

    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const override;

    Chunk getByKeysWithMode(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets,
        KeyValueLookupMode mode) const override;

    Block getSampleBlock(const Names & required_columns) const override;

private:
    Chunk getByKeysImpl(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets,
        KeyValueLookupMode mode) const;

    Names primary_key;
    Block sample_block;
    Block data_block_with_default;
    size_t rows_count = 0;
    std::vector<size_t> key_positions;
    std::unordered_map<String, size_t> column_positions;

    Arena key_to_rows_pool;
    mutable KeyToRowsMap key_to_rows;
};

}
