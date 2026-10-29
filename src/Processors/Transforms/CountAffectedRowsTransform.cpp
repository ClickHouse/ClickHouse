#include <Processors/Transforms/CountAffectedRowsTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AffectedRowsCounter::AffectedRowsCounter(size_t num_commands_)
    : num_commands(num_commands_)
    , rows_by_command_index(num_commands_ + 1)
{
}

void AffectedRowsCounter::add(const std::vector<UInt64> & affected_rows)
{
    chassert(affected_rows.size() == rows_by_command_index.size(),
    "`affected_rows` should have `num_commands + 1` elements, indexed like the totals");
    for (size_t i = 0; i < affected_rows.size(); ++i)
        rows_by_command_index[i] += affected_rows[i];
}

std::vector<UInt64> AffectedRowsCounter::get() const
{
    return rows_by_command_index;
}

namespace
{

/// Counts the rows by command index of the index column of type `T`.
template <typename T>
void countRows(const IColumn & column, std::vector<UInt64> & rows_by_command_index)
{
    for (auto command_index : assert_cast<const ColumnVector<T> &>(column).getData())
        ++rows_by_command_index[command_index];
}

}

CountAffectedRowsTransform::CountAffectedRowsTransform(
    const SharedHeader & header_, const String & command_index_column_name, AffectedRowsCounterPtr counter_)
    : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/ true)
    , command_index_column_position(header_->getPositionByName(command_index_column_name))
    , rows_by_command_index(counter_->getNumCommands() + 1)  /// slot 0 is for the rows not affected by any command
    , counter(std::move(counter_))
{
    /// The type of the index depends on the number of commands, see `getCommandIndexType` in `MutationsInterpreter`.
    const auto & type = header_->getByPosition(command_index_column_position).type;
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8: count_rows = &countRows<UInt8>; break;
        case TypeIndex::UInt16: count_rows = &countRows<UInt16>; break;
        case TypeIndex::UInt32: count_rows = &countRows<UInt32>; break;
        case TypeIndex::UInt64: count_rows = &countRows<UInt64>; break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected type {} of the column with indexes of mutation commands", type->getName());
    }
}

void CountAffectedRowsTransform::onFinish()
{
    counter->add(rows_by_command_index);
}

void CountAffectedRowsTransform::transform(Chunk & chunk)
{
    const auto & column = chunk.getColumns()[command_index_column_position];

    /// The index is constant if the conditions of the commands are constant (e.g. `UPDATE ... WHERE 1`),
    /// so all rows of the chunk are counted for one command without materializing the column.
    if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
    {
        rows_by_command_index[const_column->getUInt(0)] += chunk.getNumRows();
        return;
    }

    count_rows(*column->convertToFullIfWrapped(), rows_by_command_index);
}

}
