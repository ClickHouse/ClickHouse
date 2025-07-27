#pragma once
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/IJoin.h>

namespace DB
{

class HashJoinResult : public IJoinResult
{
public:

    struct Properties
    {
        const TableJoin & table_join;
        const Block & required_right_keys;
        const std::vector<String> & required_right_keys_sources;

        size_t max_joined_block_rows;
        size_t max_joined_block_bytes;

        size_t avg_joined_bytes_per_row;

        bool need_filter;
        bool is_join_get;
    };

    HashJoinResult(
        LazyOutput && lazy_output_,
        MutableColumns columns_,
        IColumn::Offsets offsets_,
        IColumn::Filter filter_,
        ScatteredBlock && block_,
        Properties properties_);

    JoinResultBlock next() override;

private:
    const LazyOutput lazy_output;
    const Properties properties;

    std::optional<ScatteredBlock> scattered_block;

    MutableColumns columns;
    const IColumn::Offsets offsets;
    const IColumn::Filter filter;

    size_t next_row = 0;
    size_t next_row_ref = 0;
    size_t num_joined_rows = 0;
};

}
