#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
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

/// The number of bytes before the elements in every block: the checksum and the compressed block header.
UInt64 getOneRowPerBlockHeaderBytes();

/// Whether a column of `type` can have the layout: `Array(BFloat16)`, `Array(Float32)`, `Array(Float64)` or `Array(Int8)`.
bool canHaveOneRowPerBlockLayout(const DataTypePtr & type);

/// Whether every codec of the declared codec of a column stores the data verbatim: `NONE`, or `Quantized`, which only adds
/// another stream. A column without a declared codec uses the default codec, which compresses.
bool declaredCodecStoresDataVerbatim(const ASTPtr & codec_desc);

/// The layout is recorded when a part is written, not inferred from the column settings when it is read: the settings
/// describe the parts written from now on, and a part written before they were changed, or with other buffers, can have
/// other blocks whose sizes add up to the same totals, so a computed position would land in the middle of another row.
///
/// The writer of a Wide part checks the blocks it wrote to the file of the array elements of a column: exactly one block
/// per row, every block of `row_bytes` stored without compression, and every row of `array_size` elements. Only then it
/// writes a marker file, which is in the checksums of the part. The marker holds `row_bytes`, `array_size`, and the sizes and
/// the hashes of the two files that define the layout: the file of the elements and the file of the array sizes, which
/// define where the rows begin.
///
/// The name of the marker is a hash of the name of the stream and of these two checksums, e.g. `<hash>.one_row_per_block`,
/// so its length does not depend on the name of the column. A marker is valid exactly for the bytes it was written for,
/// which is what makes it safe to carry over as is: a mutation that keeps both files keeps their marker, and one that
/// rewrites either of them changes the expected name, so the old marker is not used, and a new marker is never written
/// into one hardlinked from the source part.
constexpr auto ONE_ROW_PER_BLOCK_MARKER_EXTENSION = ".one_row_per_block";

/// The name of the marker of the file of array elements of `stream_name` with checksum `data_checksum`, and with the file of
/// array sizes with checksum `sizes_checksum`.
String getOneRowPerBlockMarkerName(
    const String & stream_name, const MergeTreeDataPartChecksum & data_checksum, const MergeTreeDataPartChecksum & sizes_checksum);

/// The content of the marker of these files.
String makeOneRowPerBlockMarker(
    UInt64 row_bytes, UInt64 array_size, const MergeTreeDataPartChecksum & data_checksum, const MergeTreeDataPartChecksum & sizes_checksum);

/// The checksum of a marker file with `content`, as recorded in the checksums of the part.
MergeTreeDataPartChecksum getOneRowPerBlockMarkerChecksum(const String & content);

/// Returns the layout of `column` in `part`, or `std::nullopt`, in which case the column must be read with the usual
/// readers.
///
/// The column must have the column-level settings `min_compress_block_size` and `max_compress_block_size` both equal to
/// the size of a row in `metadata`, which gives the number of elements of a row, and the part must have the marker written
/// for exactly this layout and this file of elements. The marker is checked against its checksum in the checksums of the
/// part, so nothing is read from disk.
std::optional<OneRowPerBlockLayout> tryGetOneRowPerBlockLayout(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const StorageInMemoryMetadata & metadata);

}
