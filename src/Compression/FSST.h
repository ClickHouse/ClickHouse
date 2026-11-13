#pragma once

#include <base/types.h>

#include <array>
#include <span>

#include <Common/VectorWithMemoryTracking.h>


namespace DB::FSST
{

/** FSST (Fast Static Symbol Table) compression, as described in
  * "FSST: Fast Random Access String Compression", Peter Boncz, Thomas Neumann, Viktor Leis, VLDB 2020.
  *
  * Data is encoded with a table of up to 255 symbols, each of 1 to 8 bytes. Every symbol is replaced with
  * its one-byte code, and a byte not covered by any symbol is written as the escape code followed by the byte.
  * The table is built from a sample of the data, so it adapts to it, and decoding is a single table lookup
  * per code. Unlike LZ-like algorithms, every code is decoded on its own, without a back reference into
  * the previously decoded data, so the encoded data can be processed without decoding it: for example,
  * values can be checked for the bytes they contain by a lookup per code (see `SerializationString`).
  */

static constexpr size_t MAX_SYMBOLS = 255;
static constexpr UInt8 ESCAPE_CODE = 255;
static constexpr size_t MAX_SYMBOL_LENGTH = 8;

/// The maximum size of a serialized symbol table.
static constexpr size_t MAX_SERIALIZED_SYMBOL_TABLE_SIZE = 1 + MAX_SYMBOLS * (1 + MAX_SYMBOL_LENGTH);

struct SymbolTable
{
    size_t num_symbols = 0;
    /// The lengths of the symbols, indexed by code. The entries for unused codes (including the escape code) are zero.
    std::array<UInt8, 256> lengths{};
    /// The bytes of the symbols, indexed by code: the first byte of a symbol is the lowest byte of the value.
    /// The bytes after the end of a symbol are zero.
    std::array<UInt64, 256> symbols{};

    /// Builds a table for the data, from a sample of it. If `value_boundaries` are given (the increasing offsets
    /// of the boundaries between the values in the data), the symbols are learned within the values.
    static SymbolTable build(const char * data, size_t size, const std::span<const UInt32> * value_boundaries = nullptr);

    size_t serializedSize() const;
    /// Writes `serializedSize` bytes: the number of symbols, their lengths, and their bytes.
    void serialize(char * dest) const;
    /// Reads a table written by `serialize` and returns the number of bytes read.
    /// Throws an exception with `error_code` on malformed data.
    size_t deserialize(const char * src, size_t size, int error_code);
};

/// The maximum size of the encoded data: every byte is escaped in the worst case.
inline constexpr size_t maxEncodedSize(size_t size) { return size * 2; }

/// Encodes the data into `dest`, which must have room for `maxEncodedSize(size)` bytes. Returns the size of the codes.
/// If `value_boundaries` are given (see `SymbolTable::build`), the encoding is aligned with the values: no symbol
/// crosses a boundary, so every value is encoded exactly as `encodeValue` encodes it alone.
size_t encode(const SymbolTable & table, const char * src, size_t size, UInt8 * dest, const std::span<const UInt32> * value_boundaries = nullptr);

/// Encodes one value, the same as `encode` with value boundaries encodes a value in the data.
/// It does not build the lookup tables of `encode`, so it is cheap for a short value.
size_t encodeValue(const SymbolTable & table, const char * src, size_t size, UInt8 * dest);

/// For the codes encoded by `encode` with `value_boundaries`, appends the position of the code at every boundary to `result`.
void findBoundaryCodePositions(
    const SymbolTable & table, const UInt8 * codes, size_t codes_size, std::span<const UInt32> value_boundaries, VectorWithMemoryTracking<UInt32> & result);

/// A position in the encoded data: the code and the number of bytes of its symbol that are already consumed.
/// The values in a block of data are encoded together, so a symbol can span several of them.
struct Cursor
{
    size_t code_pos = 0;
    size_t offset_in_symbol = 0;
};

/// Decodes `size` bytes starting at `cursor` into `dest` and advances the cursor.
/// The codes are not trusted: throws an exception with `error_code` if they end before `size` bytes,
/// or contain a code that is not in the table.
void decode(const SymbolTable & table, const UInt8 * codes, size_t codes_size, Cursor & cursor, char * dest, size_t size, int error_code);

/// Decodes all the codes, which must decode to exactly `size` bytes.
void decodeBlock(const SymbolTable & table, const UInt8 * codes, size_t codes_size, char * dest, size_t size, int error_code);

/// Advances the cursor by `size` decoded bytes without decoding them. The same checks as in `decode`.
void skip(const SymbolTable & table, const UInt8 * codes, size_t codes_size, Cursor & cursor, size_t size, int error_code);

}
