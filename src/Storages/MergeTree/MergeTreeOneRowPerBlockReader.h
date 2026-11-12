#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Storages/MergeTree/MergeTreeOneRowPerBlockLayout.h>

#include <optional>


namespace DB
{

class MergeTreeReaderStream;

/// Reads selected rows of an `Array` column of a part that has `OneRowPerBlockLayout`, see
/// `tryGetOneRowPerBlockLayout`. Every row is one block, stored without compression, at a known position in the file of
/// the array elements, so a row is read by seeking the usual stream of that file to its block and taking the block as it
/// is stored: the elements of the row are copied once, from the read buffer of the stream to the column, and only the
/// blocks of the requested rows are checksummed. The position of a row is computed, not taken from the marks or the array sizes.
///
/// The header and the checksum of every block that is read are checked, and an exception is thrown on a mismatch.
class MergeTreeOneRowPerBlockReader
{
public:
    MergeTreeOneRowPerBlockReader(OneRowPerBlockLayout layout_, UInt64 part_rows_, String path_for_logs_);

    const OneRowPerBlockLayout & getLayout() const { return layout; }

    /// Appends the rows [`first_row`, `first_row + num_rows`) of the part to `result`, an array column of the type of the
    /// column: the rows that are in `selected_rows` with their values, read from `stream`, the stream of the file of the
    /// array elements, and the other rows as empty arrays, which the caller must filter out. `selected_rows` are the
    /// numbers of rows in the part, strictly increasing.
    void readRange(
        MergeTreeReaderStream & stream, UInt64 first_row, UInt64 num_rows, const PaddedPODArray<UInt64> & selected_rows, IColumn & result);

private:
    /// Reads the block of `row` from `stream`, checks it, and copies its elements to `to`.
    void readRow(MergeTreeReaderStream & stream, UInt64 row, char * to);

    const OneRowPerBlockLayout layout;
    const UInt64 part_rows;
    const String path_for_logs;

    /// The method byte of the blocks, once it is known to store the data verbatim.
    std::optional<UInt8> verbatim_method;
};

}
