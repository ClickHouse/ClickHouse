#pragma once

#include <Processors/ISimpleTransform.h>
#include <base/defines.h>

#include <functional>


namespace DB
{

/// The number of rows affected by each of several mutation commands of a mutation stage.
/// One counter is shared by all `CountAffectedRowsTransform` of the stage. It is not thread-safe, because no parallelism
/// is needed: a part is mutated in one stream (see `max_threads` in `MutationsInterpreter`), so only one transform uses it.
/// It holds a counter with the length of `<number of commands in a stage> + 1` where the index 0 counts
/// the rows not affected by any command, and the index `i` counts the rows affected by the `i`-th command.
class AffectedRowsCounter
{
public:
    explicit AffectedRowsCounter(size_t num_commands);

    size_t getNumCommands() const { return num_commands; }

    /// Adds the counts of one transform to the totals.
    void add(const std::vector<UInt64> & affected_rows);

    /// Returns the totals, indexed by the 1-based index of the command (see above).
    std::vector<UInt64> get() const;

private:
    const size_t num_commands;

    std::vector<UInt64> rows_by_command_index;
};

using AffectedRowsCounterPtr = std::shared_ptr<AffectedRowsCounter>;

/// Counts the number of rows affected on each transform of a chunk.
/// It uses `AffectedRowsCounter` to count the rows affected by each command.
/// It does not change the chunks it processes.
class CountAffectedRowsTransform : public ISimpleTransform
{
public:
    CountAffectedRowsTransform(
        const SharedHeader & header_,
        const String & command_index_column_name,
        AffectedRowsCounterPtr counter_);

    String getName() const override { return "CountAffectedRowsTransform"; }

protected:
    void transform(Chunk & chunk) override;

    void onFinish() override;

private:
    /// Counts the rows of a full index column, chosen for the type of the column.
    std::function<void(const IColumn & column, std::vector<UInt64> & rows_by_command_index)> count_rows;

    const size_t command_index_column_position;
    std::vector<UInt64> rows_by_command_index;
    AffectedRowsCounterPtr counter;
};

}
