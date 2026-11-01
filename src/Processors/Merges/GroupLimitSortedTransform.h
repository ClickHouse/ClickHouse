#pragma once

#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace ProfileEvents
{
    extern const Event MergingSortedMilliseconds;
}

namespace DB
{

/// Merges sorted inputs into the input rows of each group's window, discarding seen rows (see
/// `GroupLimitSortedAlgorithm`): the spill runs of a group limit, or the runs of a sort that keeps one row per key.
/// `name_` is the processor name shown to users, which tells the operators apart.
class GroupLimitSortedTransform final : public IMergingTransform<GroupLimitSortedAlgorithm>
{
public:
    GroupLimitSortedTransform(
        String name_, SharedHeaders input_headers, SharedHeader output_header, SortDescription key_description,
        const std::optional<String> & seen_rows_column, UInt64 group_offset, UInt64 group_limit_end,
        size_t max_block_size_rows, bool have_all_inputs = true)
        : IMergingTransform(
            input_headers, output_header, have_all_inputs, /*limit_hint_=*/ 0,
            /*always_read_till_end_=*/ false, /*empty_chunk_on_finish_=*/ false,
            input_headers, output_header, std::move(key_description), seen_rows_column, group_offset, group_limit_end,
            max_block_size_rows)
        , name(std::move(name_))
    {
    }

    String getName() const override { return name; }

protected:
    void onNewInput() override { algorithm.addInput(inputs.back().getSharedHeader()); }
    void onFinish() override
    {
        logMergedStats(ProfileEvents::MergingSortedMilliseconds, "Merged", getLogger(name));
    }

private:
    const String name;
};

}
