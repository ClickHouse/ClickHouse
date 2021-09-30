#pragma once

#include <Common/COW.h>
#include <Core/Types.h>

#include <unordered_map>
#include <memory>

namespace DB
{

class IDataType;

class ReadBuffer;
class WriteBuffer;
class ProtobufReader;
class ProtobufWriter;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class Field;

struct FormatSettings;
struct NameAndTypePair;

class ISerialization
{
public:
    ISerialization() = default;
    virtual ~ISerialization() = default;

    /** Binary serialization for range of values in column - for writing to disk/network, etc.
      *
      * Some data types are represented in multiple streams while being serialized.
      * Example:
      * - Arrays are represented as stream of all elements and stream of array sizes.
      * - Nullable types are represented as stream of values (with unspecified values in place of NULLs) and stream of NULL flags.
      *
      * Different streams are identified by "path".
      * If the data type require single stream (it's true for most of data types), the stream will have empty path.
      * Otherwise, the path can have components like "array elements", "array sizes", etc.
      *
      * For multidimensional arrays, path can have arbitrary length.
      * As an example, for 2-dimensional arrays of numbers we have at least three streams:
      * - array sizes;                      (sizes of top level arrays)
      * - array elements / array sizes;     (sizes of second level (nested) arrays)
      * - array elements / array elements;  (the most deep elements, placed contiguously)
      *
      * Descendants must override either serializeBinaryBulk, deserializeBinaryBulk methods (for simple cases with single stream)
      *  or serializeBinaryBulkWithMultipleStreams, deserializeBinaryBulkWithMultipleStreams, enumerateStreams methods (for cases with multiple streams).
      *
      * Default implementations of ...WithMultipleStreams methods will call serializeBinaryBulk, deserializeBinaryBulk for single stream.
      */

    struct Substream
    {
        enum Type
        {
            ArrayElements,
            ArraySizes,

            NullableElements,
            NullMap,

            TupleElement,

            DictionaryKeys,
            DictionaryIndexes,

            SparseElements,
            SparseOffsets,
        };
        Type type;

        /// Index of tuple element, starting at 1 or name.
        String tuple_element_name;

        /// Do we need to escape a dot in filenames for tuple elements.
        bool escape_tuple_delimiter = true;

        Substream(Type type_) : type(type_) {}

        String toString() const;
    };

    struct SubstreamPath : public std::vector<Substream>
    {
        String toString() const;
    };

    /// Cache for common substreams of one type, but possible different its subcolumns.
    /// E.g. sizes of arrays of Nested data type.
    using SubstreamsCache = std::unordered_map<String, ColumnPtr>;

    using StreamCallback = std::function<void(const SubstreamPath &)>;

    virtual void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const;
    void enumerateStreams(const StreamCallback & callback, SubstreamPath && path) const { enumerateStreams(callback, path); }
    void enumerateStreams(const StreamCallback & callback) const { enumerateStreams(callback, {}); }

    using OutputStreamGetter = std::function<WriteBuffer*(const SubstreamPath &)>;
    using InputStreamGetter = std::function<ReadBuffer*(const SubstreamPath &)>;

    struct SerializeBinaryBulkState
    {
        virtual ~SerializeBinaryBulkState() = default;
    };

    struct DeserializeBinaryBulkState
    {
        virtual ~DeserializeBinaryBulkState() = default;
    };

    using SerializeBinaryBulkStatePtr = std::shared_ptr<SerializeBinaryBulkState>;
    using DeserializeBinaryBulkStatePtr = std::shared_ptr<DeserializeBinaryBulkState>;

    struct SerializeBinaryBulkSettings
    {
        OutputStreamGetter getter;
        SubstreamPath path;

        size_t low_cardinality_max_dictionary_size = 0;
        bool low_cardinality_use_single_dictionary_for_part = true;

        bool position_independent_encoding = true;
    };

    struct DeserializeBinaryBulkSettings
    {
        InputStreamGetter getter;
        SubstreamPath path;

        /// True if continue reading from previous positions in file. False if made fseek to the start of new granule.
        bool continuous_reading = true;

        bool position_independent_encoding = true;

        bool native_format = false;

        /// If not zero, may be used to avoid reallocations while reading column of String type.
        double avg_value_size_hint = 0;
    };

    /// Call before serializeBinaryBulkWithMultipleStreams chain to write something before first mark.
    virtual void serializeBinaryBulkStatePrefix(
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call after serializeBinaryBulkWithMultipleStreams chain to finish serialization.
    virtual void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call before before deserializeBinaryBulkWithMultipleStreams chain to get DeserializeBinaryBulkStatePtr.
    virtual void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & /*state*/) const {}

    /** 'offset' and 'limit' are used to specify range.
      * limit = 0 - means no limit.
      * offset must be not greater than size of column.
      * offset + limit could be greater than size of column
      *  - in that case, column is serialized till the end.
      */
    virtual void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// Read no more than limit values and append them into column.
    virtual void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;

    /** Override these methods for data types that require just single stream (most of data types).
      */
    virtual void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const;
    virtual void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const;

    /** Serialization/deserialization of individual values.
      *
      * These are helper methods for implementation of various formats to input/output for user (like CSV, JSON, etc.).
      * There is no one-to-one correspondence between formats and these methods.
      * For example, TabSeparated and Pretty formats could use same helper method serializeTextEscaped.
      *
      * For complex data types (like arrays) binary serde for individual values may differ from bulk serde.
      * For example, if you serialize single array, it will be represented as its size and elements in single contiguous stream,
      *  but if you bulk serialize column with arrays, then sizes and elements will be written to separate streams.
      */

    /// There is two variants for binary serde. First variant work with Field.
    virtual void serializeBinary(const Field & field, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(Field & field, ReadBuffer & istr) const = 0;

    /// Other variants takes a column, to avoid creating temporary Field object.
    /// Column must be non-constant.

    /// Serialize one value of a column at specified row number.
    virtual void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
    /// Deserialize one value and insert into a column.
    /// If method will throw an exception, then column will be in same state as before call to method.
    virtual void deserializeBinary(IColumn & column, ReadBuffer & istr) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization as a literal that may be inserted into a query.
      */
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for the CSV format.
      */
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    /** Text deserialization in case when buffer contains only one value, without any escaping and delimiters.
      */
    virtual void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization intended for using in JSON format.
      */
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for putting into the XML format.
      */
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        serializeText(column, row_num, ostr, settings);
    }

    static String getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path);
    static String getFileNameForStream(const String & name_in_storage, const SubstreamPath & path);
    static String getSubcolumnNameForStream(const SubstreamPath & path);

    static void addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column);
    static ColumnPtr getFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path);

    static bool isSpecialCompressionAllowed(const SubstreamPath & path);
};

using SerializationPtr = std::shared_ptr<const ISerialization>;
using Serializations = std::vector<SerializationPtr>;

}
