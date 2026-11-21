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

class ReadBuffer;
class WriteBuffer;

/// The record of one granule of a `String` column written with dictionary granules
/// (`MergeTreeStringSerializationVersion::WITH_DICTIONARIES`), in the stream of substream `StringDictionaryIndexes`:
///
///     UInt8    kind              0 = plain, 1 = dictionary
///     VarUInt  rows              rows of the granule
///     only for a dictionary granule:
///     VarUInt  dictionary_size   number of distinct values, at least 1
///     UInt8    code_width        1, 2 or 4: the fewest bytes that hold dictionary_size - 1
///     bytes    codes             rows * code_width bytes, little-endian
///
/// The rows of a plain granule are in the streams `StringSizes` and `Regular`, exactly as with `WITH_SIZE_STREAM`. The
/// values of a dictionary are sorted in byte order and unique, so the empty string, when present, is code 0. Their
/// lengths are in `StringDictionarySizes` (`UInt64` each) and their bytes in `StringDictionaryElements`.
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
    UInt8 code_width = 0;

    /// Writes everything before the codes.
    void write(WriteBuffer & out) const;

    /// Reads everything before the codes and rejects a header that no writer produces.
    void read(ReadBuffer & in);

    /// Returns the size of the header as `write` writes it.
    size_t byteSize() const;

    static UInt8 codeWidthFor(UInt64 dictionary_size);
};

/// Builds the dictionary form of one granule of a `String` column and decides whether to store the granule in it.
/// The decision compares exact sizes, because the whole granule is in memory: the dictionary form is used when it is
/// smaller than the plain form by at least `min_size_reduction` of the plain size, and when the distinct values are at
/// most `max_distinct_ratio` of the rows. Both forms count 8 bytes per stored length:
///
///     plain      = bytes of all rows + 8 * rows
///     dictionary = bytes of the distinct values + 8 * distinct + code_width * rows + the header
///
/// Example: 8192 rows of 3 values of 100 bytes give plain = 884,736 and dictionary = 324 + 8,192 + 5 = 8,521 bytes.
///
/// The builder keeps its buffers between granules, so one builder serves all granules of a column.
class StringGranuleDictionaryBuilder
{
public:
    /// Rows `[offset, offset + rows)` of a `ColumnString`, one piece of the granule.
    struct Slice
    {
        ColumnPtr column;
        size_t offset = 0;
        size_t rows = 0;
    };

    struct Settings
    {
        double min_size_reduction = 0.1;
        double max_distinct_ratio = 0.75;
    };

    /// Builds the dictionary of the granule formed by `slices`, which hold `rows` rows in total, and returns whether the
    /// granule is stored as a dictionary. After `true`, `getDictionary` and `writeCodes` describe the granule.
    bool build(const std::vector<Slice> & slices, size_t rows, const Settings & settings);

    const ColumnString & getDictionary() const { return *dictionary; }
    UInt8 getCodeWidth() const { return code_width; }

    /// Writes the code of every row, `getCodeWidth` bytes each, little-endian.
    void writeCodes(WriteBuffer & out) const;

private:
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

    ColumnString::MutablePtr dictionary = ColumnString::create();
    UInt8 code_width = 0;
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
    /// The values of the current dictionary granule, read by the column.
    ColumnPtr dictionary;
    /// The lengths of these values, read by the `.size` subcolumn alone.
    PaddedPODArray<UInt64> dictionary_sizes;

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override;
};

/// Returns the deserialization state that the column and its `.size` subcolumn share through `cache`, creating it on the
/// first call.
ISerialization::DeserializeBinaryBulkStatePtr getStringGranulesDeserializeState(
    ISerialization::DeserializeBinaryBulkSettings & settings, ISerialization::SubstreamsDeserializeStatesCache * cache);

/// Reads the codes of `rows` rows of the dictionary granule `granule` into a column of the unsigned type of the code
/// width, which `ColumnIndex` accepts as is, and rejects a code outside the dictionary.
MutableColumnPtr readStringGranuleCodes(ReadBuffer & in, const StringGranuleHeader & granule, size_t rows);

}
