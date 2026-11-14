#pragma once

#include <Core/NamesAndTypes.h>
#include <IO/ReadSettings.h>
#include <base/types.h>

#include <optional>


namespace DB
{

class IMergeTreeDataPart;
struct StorageInMemoryMetadata;

/// The layout of an `Array` column in a part where every row has the same number of elements and is
/// stored as exactly one uncompressed block in the file of the array elements. The position of any row
/// in that file is then known without reading marks or array sizes: the block of row `i` starts at
/// `i * block_bytes`, and its elements follow the block header, at `i * block_bytes + header_bytes`.
struct OneRowPerBlockLayout
{
    /// The file of the array elements in the part, e.g. `vec.bin`.
    String file_name;
    /// The number of elements in every row.
    UInt64 array_size = 0;
    /// The number of bytes of the elements of a row: `array_size` multiplied by the size of an element.
    UInt64 row_bytes = 0;
    /// The number of bytes before the elements in every block: the checksum and the compressed block header.
    UInt64 header_bytes = 0;
    /// The number of bytes of a block in the file: `header_bytes + row_bytes`.
    UInt64 block_bytes = 0;
};

/// Whether a column of `type` can have the layout: `Array(BFloat16)`, `Array(Float32)`, `Array(Float64)` or `Array(Int8)`.
bool canHaveOneRowPerBlockLayout(const DataTypePtr & type);

/// Returns the layout that `column` of `part` has if every row of it has the same number of elements, or `std::nullopt`
/// if it cannot have the layout, in which case the column must be read with the usual readers.
///
/// The column must have the column-level settings `min_compress_block_size` and `max_compress_block_size` both equal to
/// the size of a row in `metadata`, which gives the number of elements of a row, and the sizes of the file of the array
/// elements recorded in the checksums of the part must be those of one block per row. Nothing is read from disk, so the
/// caller must still check that every row has `array_size` elements, see `checkOneRowPerBlockArraySizes`.
std::optional<OneRowPerBlockLayout> tryGetOneRowPerBlockLayout(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const StorageInMemoryMetadata & metadata);

/// Whether every row of `column` in `part` has exactly `array_size` elements, the second condition of the layout, which the
/// sizes of the files checked by `tryGetOneRowPerBlockLayout` cannot tell: rows of different lengths whose total is the
/// same also fill a file of the same size, e.g. rows of 63 and 65 elements in blocks of 64, and then the block at the
/// position of a row would hold parts of other rows, read without any error.
///
/// Reads the sizes of the arrays, the stream `.size0` with one `UInt64` per row, until the first row of another size.
/// The stream is read with a buffer of its own, so the streams of the readers of the part keep their positions.
bool checkOneRowPerBlockArraySizes(
    const IMergeTreeDataPart & part, const NameAndTypePair & column, UInt64 array_size, const ReadSettings & read_settings);

}
