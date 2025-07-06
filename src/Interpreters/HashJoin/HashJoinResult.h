#pragma once
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/IJoin.h>

namespace DB
{

class HashJoinResult : public IJoinResult
{
    LazyOutput lazy_output;

    const HashJoin * join;

    bool need_filter;
    bool is_join_get;
    bool is_asof_join;
    std::optional<ScatteredBlock> scattered_block;

    static void appendRightColumns(
        Block & block,
        const HashJoin * join,
        MutableColumns columns,
        const NamesAndTypes & type_name,
        IColumn::Filter filter,
        IColumn::Offsets offsets_to_replicate,
        bool need_filter,
        bool is_asof_join);

public:
    HashJoinResult(
        LazyOutput && lazy_output_,
        bool need_filter_,
        bool is_join_get_,
        bool is_asof_join_,
        ScatteredBlock && block_,
        const HashJoin * join_);

    JoinResultBlock next() override;
};

}
