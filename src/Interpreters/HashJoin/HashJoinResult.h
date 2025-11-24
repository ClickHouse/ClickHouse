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

        bool joined_block_split_single_row = false;

        bool enable_lazy_columns_replication = false;
    };

    HashJoinResult(
        LazyOutput && lazy_output_,
        MutableColumns columns_,
        IColumn::Offsets offsets_,
        IColumn::Filter filter_,
        IColumn::Offsets && matched_rows_,
        ScatteredBlock && block_,
        Properties properties_);

    JoinResultBlock next() override;

    ~HashJoinResult() override;
private:
    const LazyOutput lazy_output;
    const Properties properties;

    std::optional<ScatteredBlock> scattered_block;

    MutableColumns columns;
    IColumn::Offsets offsets;
    IColumn::Filter filter;
    IColumn::Offsets matched_rows;

    size_t next_row = 0;
    size_t next_matched_rows_it = 0;
    size_t next_row_ref = 0;
    size_t num_joined_rows = 0;

    struct GenerateCurrentRowState
    {
        Block block;
        size_t rows_to_reserve;
        size_t row_ref_begin;
        size_t row_ref_end;
        MutableColumns columns;

        IColumn::Offsets offsets;
        IColumn::Filter filter;

        std::span<UInt64> matched_rows;

        bool is_last;

        /// If non-zero, limits the number of rows outputted from the block.
        size_t state_row_limit;
        /// If non-zero, limits the number of bytes outputted from the block.
        /// Works only when state_row_limit is also non-zero.
        size_t state_bytes_limit;

        /// Rows already outputted from the block, which are skipped on next call to generateBlock.
        size_t state_row_offset = 0;
    };

    /// HashJoinResult iterates over rows from the left side.
    /// This state is used to generate blocks for a single row from the left side.
    /// When limiting the number of rows in a block, if there are many matches for a single key,
    /// the current progress is saved here to continue from this state on the next call to next().
    std::optional<GenerateCurrentRowState> current_row_state;

    static Block generateBlock(std::optional<GenerateCurrentRowState> & state, const LazyOutput & lazy_output, const Properties & properties);
};

}
