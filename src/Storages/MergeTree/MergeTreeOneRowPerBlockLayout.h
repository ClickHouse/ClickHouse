#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <optional>


namespace DB
{

class Block;
class ColumnsDescription;
class IMergeTreeDataPart;
struct ColumnDescription;
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

/// Whether the declared codec of a column is a single codec that stores the data verbatim: `NONE`, or `Quantized`, which
/// only adds another stream. Returns false for a chain of several codecs and for a column without a declared codec.
bool declaredCodecStoresDataVerbatim(const ASTPtr & codec_desc);

/// The column-level setting that declares the number of elements of every row of a column and makes the elements of every
/// row be written as one uncompressed block. It can be specified only in `CREATE TABLE`, so every part of the table is
/// written with it, and the writer of a Wide part enforces it, see `MergeTreeDataPartWriterWide`.
constexpr auto UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING = "uncompressed_fixed_array_size";

/// The value of the setting `uncompressed_fixed_array_size` of `column`, or 0 if it does not have it.
UInt64 getUncompressedFixedArraySize(const ColumnDescription & column);

/// Checks that a column declared in `CREATE TABLE` can have the setting `uncompressed_fixed_array_size`, if it has it.
void validateUncompressedFixedArraySize(const ColumnDescription & column);

/// Throws an exception if a row of a column of `block` with the setting `uncompressed_fixed_array_size` in `metadata` does not
/// have the declared number of elements. Called by the writers of all part types, so that a part of any type has only such
/// rows, and merges and mutations can always write them as one block per row.
void checkUncompressedFixedArraySizes(const Block & block, const StorageInMemoryMetadata & metadata);

/// Throws `NOT_IMPLEMENTED` if a column of `columns` has the setting `uncompressed_fixed_array_size`. The parts of such a
/// table are read at positions computed from the setting, so `operation`, which would bring in parts that this table did
/// not write, is not supported for it.
void checkNoUncompressedFixedArrayColumns(const ColumnsDescription & columns, std::string_view operation);

/// Whether `path` is the path of the stream of the elements of a top-level `Array` of numbers: `ArrayElements`, `Regular`.
bool isTopLevelArrayElementsStream(const ISerialization::SubstreamPath & path);

/// Whether a column of `metadata`, or a virtual column, has a default expression of any kind, `DEFAULT`, `MATERIALIZED`,
/// `ALIAS` or `EPHEMERAL`, with an identifier, which could name another column. Any identifier counts, without resolving
/// what it names, e.g. `vec`, `dummy.vec`, `n.d` or the parameter of a lambda. Expressions such as `now()` or literals do
/// not count. A reader fills a column that a part does not have from its default expression, see
/// `IMergeTreeReader::evaluateMissingDefaults`, which follows the columns that an expression names, of every kind.
bool hasDefaultExpressionWithIdentifiers(const StorageInMemoryMetadata & metadata);

/// Returns the layout of `column` in `part`, or `std::nullopt`, in which case the column must be read with the usual
/// readers: if the column does not have the setting `uncompressed_fixed_array_size` in `metadata`, or the part is not a
/// Wide part, or the part does not have the column. Throws an exception if the sizes of the file of the elements in the
/// checksums of the part contradict the setting. Nothing is read from disk.
std::optional<OneRowPerBlockLayout> tryGetOneRowPerBlockLayout(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const StorageInMemoryMetadata & metadata);

}
