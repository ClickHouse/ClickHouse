#pragma once

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/PODArray.h>

#include <string_view>
#include <vector>

namespace DB
{

class ColumnReplicated;
class ReadBuffer;
class WriteBuffer;

/// The record of one granule of a `String` column written with dictionary granules
/// (`MergeTreeStringSerializationVersion::WITH_DICTIONARIES`), in the stream of substream
/// `StringDictionaryIndexes`:
///
///     UInt8    kind              0 = plain, 1 = dictionary
///     VarUInt  rows              rows of the granule
///     only for a dictionary granule:
///     VarUInt  dictionary_size   number of distinct values, at least 1
///     UInt8    code_bits         the fewest bits that hold dictionary_size - 1, so 0 for a single value
///     bytes    codes             the code of every row in code_bits bits, packed from the lowest bit of
///                                the first byte: ceil(rows * code_bits / 8) bytes
///
/// The rows of a plain granule are in the streams `StringSizes` and `Regular`, exactly as with
/// `WITH_SIZE_STREAM`. The values of a dictionary are sorted in byte order and unique, so the empty string,
/// when present, is code 0. Their lengths are in `StringDictionarySizes` (`UInt64` each) and their bytes in
/// `StringDictionaryElements`.
///
/// Example: 8192 rows of 5 distinct values take 3 bits per row, 3072 bytes of codes.
struct StringGranuleHeader
{
    enum class Kind : UInt8
    {
        Plain = 0,
        Dictionary = 1,
    };

    Kind kind = Kind::Plain;
    UInt64 rows = 0;
    UInt64 dictionary_size = 0;
    UInt8 code_bits = 0;

    /// Writes everything before the codes.
    void write(WriteBuffer & out) const;

    /// Reads everything before the codes and rejects a header that no writer produces.
    void read(ReadBuffer & in);

    /// Returns the size of the packed codes of a dictionary granule.
    size_t codesByteSize() const;

    static UInt8 codeBitsFor(UInt64 dictionary_size);
};

/// Builds the dictionary form of one granule of a `String` column and decides whether to store the granule in
/// it. The decision compares the sizes of the two forms with every integer in the fewest bits that hold the
/// largest one, as the codes are packed and as an integer codec stores the lengths:
///
///     plain      = bytes of all rows + rows * bits(longest length)
///     dictionary = bytes of the distinct values + distinct * bits(longest length) + rows * code_bits
///
/// (bits converted to bytes and rounded up). The dictionary form is used when it is not larger than the plain
/// form reduced by `min_size_reduction` of it, and when the distinct values are at most `max_distinct_ratio`
/// of the rows. Every term of the dictionary form grows with the distinct values, so the builder stops at the
/// first value that breaks a limit: the granule then stays plain.
///
/// Example: 8192 rows of 820 values, one of them empty in 90% of the rows and the others 64 bytes long, give
/// plain = 52,416 + 7,168 = 59,584 and dictionary = 52,416 + 718 + 10,240 = 63,374 bytes, so the granule
/// stays plain. 8192 rows of 8 values of up to 7 bytes give plain = 28,672 + 3,072 and
/// dictionary = 28 + 3 + 3,072 bytes.
///
/// The builder takes slices of a `ColumnString`, or of a `ColumnReplicated` whose nested column is a
/// `ColumnString`, as a part read with dictionary granules gives. A replicated slice is hashed once per row
/// of its nested column that its rows use (a slot), not once per row.
///
/// The builder keeps its buffers between granules, so one builder serves all granules of a column.
class StringGranuleDictionaryBuilder
{
public:
    /// Rows `[offset, offset + rows)` of a `ColumnString` or of a `ColumnReplicated` over a `ColumnString`,
    /// one piece of the granule.
    struct Slice
    {
        ColumnPtr column;
        size_t offset = 0;
        size_t rows = 0;
    };

    struct Settings
    {
        double min_size_reduction = 0;
        double max_distinct_ratio = 0.75;
    };

    /// Builds the dictionary of the granule formed by `slices`, which hold `rows` rows in total, and returns whether the
    /// granule is stored as a dictionary. After `true`, `getDictionary` and `writeCodes` describe the granule.
    bool build(const std::vector<Slice> & slices, size_t rows, const Settings & settings);

    const ColumnString & getDictionary() const { return *dictionary; }
    UInt8 getCodeBits() const { return code_bits; }

    /// Writes the code of every row, packed in `getCodeBits` bits each.
    void writeCodes(WriteBuffer & out);

private:
    /// Gives `value` its position in `values` in `position`. Returns false once the granule certainly stays
    /// plain.
    bool addValue(std::string_view value, UInt32 & position);
    /// Add the rows of a slice, assigning `row_codes` from `row` on. Return false once the granule certainly
    /// stays plain.
    bool addPlainRows(const ColumnString & column, size_t offset, size_t rows, size_t & row);
    bool addReplicatedRows(const ColumnReplicated & column, size_t offset, size_t rows, size_t & row);

    using Positions = ClearableHashMap<
        std::string_view,
        UInt32,
        DefaultHash<std::string_view>,
        ClearableHashMapCellWithSavedHash<std::string_view, UInt32, DefaultHash<std::string_view>>>;

    /// Distinct value -> its position in `values`. Clearing it costs nothing, and it keeps the size that the granule
    /// with the most distinct values needed.
    Positions positions;
    /// Distinct values in the order of their first row.
    std::vector<std::string_view> values;
    /// For every row, the position of its value in `values`; replaced by its code after the sort.
    PaddedPODArray<UInt32> row_codes;
    /// Position in `values` -> code, the position in the sorted dictionary.
    PaddedPODArray<UInt32> codes_of_positions;
    /// Positions in `values`, sorted by value.
    PaddedPODArray<UInt32> sorted_positions;

    /// The granule being built: its rows, the bytes of its distinct values, the bits of its longest length,
    /// and the limits of the dictionary form.
    size_t granule_rows = 0;
    size_t dictionary_bytes = 0;
    UInt8 length_bits = 0;
    double max_dictionary_size = 0;
    double max_distinct = 0;

    /// For each slot of the nested column of a replicated slice, the position in `values` of its value, valid
    /// where `slot_generations` holds the current `generation`, so that a new slice does not clear the
    /// arrays.
    PaddedPODArray<UInt32> slot_positions;
    PaddedPODArray<UInt32> slot_generations;
    UInt32 generation = 0;

    /// The packed codes of the granule being written.
    PaddedPODArray<UInt8> packed_codes;

    ColumnString::MutablePtr dictionary = ColumnString::create();
    UInt8 code_bits = 0;
};

/// The serialization state of a column with dictionary granules: the rows of the granule being written. The writer passes
/// a granule in one call, or in several when it spans blocks, and the granule is written once it is complete.
struct SerializeBinaryBulkStateStringWithDictionaries : public ISerialization::SerializeBinaryBulkState
{
    std::vector<StringGranuleDictionaryBuilder::Slice> slices;
    size_t rows = 0;
    StringGranuleDictionaryBuilder::Settings settings;
    StringGranuleDictionaryBuilder builder;
};

/// The deserialization state of a column with dictionary granules. The column and its `.size` subcolumn share it through
/// the states cache, because they read the same streams: when the column itself is read (`need_string_data`), the
/// subcolumn takes the lengths from it, otherwise the subcolumn reads the lengths alone.
struct DeserializeBinaryBulkStateStringWithDictionaries : public ISerialization::DeserializeBinaryBulkState
{
    bool need_string_data = false;

    /// The granule being read and the number of its rows not read yet.
    StringGranuleHeader granule;
    size_t rows_left_in_granule = 0;
    /// The codes of all rows of the current dictionary granule, decoded when the granule starts, in a column
    /// of the unsigned type of the fewest bytes that hold them, which `ColumnIndex` takes as is. A reader may
    /// move them out while it reads the granule, and keeps them here between calls that read it.
    MutableColumnPtr granule_codes;
    /// The values of the current dictionary granule, read by the column.
    ColumnPtr dictionary;
    /// The lengths of these values, read by the `.size` subcolumn alone.
    PaddedPODArray<UInt64> dictionary_sizes;

    /// Reads the record of the next granule from `indexes`, the stream of `StringDictionaryIndexes`, and
    /// decodes the codes of a dictionary granule into `granule_codes`. Forgets the dictionary of the previous
    /// granule. Returns false at the end of the stream.
    bool startGranule(ReadBuffer & indexes);

    /// The row of the current granule that the next read starts at.
    size_t firstUnreadRow() const { return granule.rows - rows_left_in_granule; }

    /// Takes back a column of codes that a reader moved out of `granule_codes`, for the codes of a later
    /// granule.
    void recycleCodes(MutableColumnPtr && codes);

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override;

private:
    /// Columns of codes for later granules.
    std::vector<MutableColumnPtr> spare_codes;
    /// The packed codes of the current granule as read, before decoding.
    PaddedPODArray<UInt8> packed_codes;
};

/// Returns the deserialization state that the column and its `.size` subcolumn share through `cache`, creating it on the
/// first call.
ISerialization::DeserializeBinaryBulkStatePtr getStringGranulesDeserializeState(
    ISerialization::DeserializeBinaryBulkSettings & settings, ISerialization::SubstreamsDeserializeStatesCache * cache);

}
