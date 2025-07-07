#pragma once
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/IJoin.h>

namespace DB
{

class HashJoinResult : public IJoinResult
{
public:
    HashJoinResult(
        LazyOutput && lazy_output_,
        MutableColumns columns_,
        IColumn::Offsets offsets_to_replicate_,
        IColumn::Filter filter_,
        bool need_filter_,
        bool is_join_get_,
        bool is_asof_join_,
        ScatteredBlock && block_,
        const HashJoin * join_);

    JoinResultBlock next() override;

    struct Data
    {
        MutableColumns columns;
        IColumn::Offsets offsets_to_replicate;
        IColumn::Filter filter;
    };

private:
    LazyOutput lazy_output;

    std::optional<ScatteredBlock> scattered_block;

    Data data;
    size_t next_row = 0;
    size_t next_row_ref = 0;
    size_t num_rows_to_join = 0;

    const HashJoin * join;

    bool need_filter;
    bool is_join_get;
    bool is_asof_join;
};

}
