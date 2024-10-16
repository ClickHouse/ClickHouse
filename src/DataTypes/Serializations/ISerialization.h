#pragma once

#include <Common/COW.h>
#include <Core/Types_fwd.h>
#include <base/demangle.h>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <memory>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class IDataType;

class ReadBuffer;
class WriteBuffer;
class ProtobufReader;
class ProtobufWriter;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;

class SerializationInfo;
using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;

class Field;

struct FormatSettings;
struct NameAndTypePair;

/** Represents serialization of data type.
 *  Has methods to serialize/deserialize column in binary and several text formats.
 *  Every data type has default serialization, but can be serialized in different representations.
 *  Default serialization can be wrapped to one of the special kind of serializations.
 *  Currently there is only one special serialization: Sparse.
 *  Each serialization has its own implementation of IColumn as its in-memory representation.
 */
class ISerialization : private boost::noncopyable, public std::enable_shared_from_this<ISerialization>
{
public:
    ISerialization() = default;
    virtual ~ISerialization() = default;

    enum class Kind : UInt8
    {
        DEFAULT = 0,
        SPARSE = 1,
    };

    virtual Kind getKind() const { return Kind::DEFAULT; }
    SerializationPtr getPtr() const { return shared_from_this(); }

    static Kind getKind(const IColumn & column);
    static String kindToString(Kind kind);
    static Kind stringToKind(const String & str);

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

    struct ISubcolumnCreator
    {
        virtual DataTypePtr create(const DataTypePtr & prev) const = 0;
        virtual SerializationPtr create(const SerializationPtr & prev) const = 0;
        virtual ColumnPtr create(const ColumnPtr & prev) const = 0;
        virtual ~ISubcolumnCreator() = default;
    };

    using SubcolumnCreatorPtr = std::shared_ptr<const ISubcolumnCreator>;

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

    struct SubstreamData
    {
        SubstreamData() = default;
        explicit SubstreamData(SerializationPtr serialization_)
            : serialization(std::move(serialization_))
        {
        }

        SubstreamData & withType(DataTypePtr type_)
        {
            type = std::move(type_);
            return *this;
        }

        SubstreamData & withColumn(ColumnPtr column_)
        {
            column = std::move(column_);
            return *this;
        }

        SubstreamData & withSerializationInfo(SerializationInfoPtr serialization_info_)
        {
            serialization_info = std::move(serialization_info_);
            return *this;
        }

        SubstreamData & withDeserializeState(DeserializeBinaryBulkStatePtr deserialize_state_)
        {
            deserialize_state = std::move(deserialize_state_);
            return *this;
        }

        SerializationPtr serialization;
        DataTypePtr type;
        ColumnPtr column;
        SerializationInfoPtr serialization_info;

        /// For types with dynamic subcolumns deserialize state contains information
        /// about current dynamic structure. And this information can be useful
        /// when we call enumerateStreams after deserializeBinaryBulkStatePrefix
        /// to enumerate dynamic streams.
        DeserializeBinaryBulkStatePtr deserialize_state;
    };

    struct Substream
    {
        enum Type
        {
            ArrayElements,
            ArraySizes,

            NullableElements,
            NullMap,

            TupleElement,
            NamedOffsets,
            NamedNullMap,

            DictionaryKeys,
            DictionaryIndexes,

            SparseElements,
            SparseOffsets,

            DeprecatedObjectStructure,
            DeprecatedObjectData,

            VariantDiscriminators,
            NamedVariantDiscriminators,
            VariantOffsets,
            VariantElements,
            VariantElement,
            VariantElementNullMap,

            DynamicData,
            DynamicStructure,

            ObjectData,
            ObjectTypedPath,
            ObjectDynamicPath,
            ObjectSharedData,
            ObjectStructure,

            Regular,
        };

        /// Types of substreams that can have arbitrary name.
        static const std::set<Type> named_types;

        Type type = Type::Regular;

        /// The name of a variant element type.
        String variant_element_name;

        /// Name of substream for type from 'named_types'.
        String name_of_substream;

        /// Path name for Object type elements.
        String object_path_name;

        /// Data for current substream.
        SubstreamData data;

        /// Creator of subcolumn for current substream.
        SubcolumnCreatorPtr creator = nullptr;

        /// Flag, that may help to traverse substream paths.
        mutable bool visited = false;

        Substream() = default;
        Substream(Type type_) : type(type_) {} /// NOLINT
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

    struct EnumerateStreamsSettings
    {
        SubstreamPath path;
        bool position_independent_encoding = true;
        /// If set to false, don't enumerate dynamic subcolumns
        /// (such as dynamic types in Dynamic column or dynamic paths in JSON column).
        /// It may be needed when dynamic subcolumns are processed separately.
        bool enumerate_dynamic_streams = true;
    };

    virtual void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const;

    /// Enumerate streams with default settings.
    void enumerateStreams(
        const StreamCallback & callback,
        const DataTypePtr & type = nullptr,
        const ColumnPtr & column = nullptr) const;

    using OutputStreamGetter = std::function<WriteBuffer*(const SubstreamPath &)>;
    using InputStreamGetter = std::function<ReadBuffer*(const SubstreamPath &)>;

    struct SerializeBinaryBulkSettings
    {
        OutputStreamGetter getter;
        SubstreamPath path;

        size_t low_cardinality_max_dictionary_size = 0;
        bool low_cardinality_use_single_dictionary_for_part = true;

        bool position_independent_encoding = true;

        /// True if data type names should be serialized in binary encoding.
        bool data_types_binary_encoding = false;

        bool use_compact_variant_discriminators_serialization = false;

        /// Serialize JSON column as single String column with serialized JSON values.
        bool write_json_as_string = false;

        enum class ObjectAndDynamicStatisticsMode
        {
            NONE,   /// Don't write statistics.
            PREFIX, /// Write statistics in prefix.
            SUFFIX, /// Write statistics in suffix.
        };
        ObjectAndDynamicStatisticsMode object_and_dynamic_write_statistics = ObjectAndDynamicStatisticsMode::NONE;
    };

    struct DeserializeBinaryBulkSettings
    {
        InputStreamGetter getter;
        SubstreamPath path;

        /// True if continue reading from previous positions in file. False if made fseek to the start of new granule.
        bool continuous_reading = true;

        bool position_independent_encoding = true;

        /// True if data type names should be deserialized in binary encoding.
        bool data_types_binary_encoding = false;

        bool native_format = false;

        /// If not zero, may be used to avoid reallocations while reading column of String type.
        double avg_value_size_hint = 0;

        bool object_and_dynamic_read_statistics = false;
    };

    /// Call before serializeBinaryBulkWithMultipleStreams chain to write something before first mark.
    /// Column may be used only to retrieve the structure.
    virtual void serializeBinaryBulkStatePrefix(
        const IColumn & /*column*/,
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call after serializeBinaryBulkWithMultipleStreams chain to finish serialization.
    virtual void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    using SubstreamsDeserializeStatesCache = std::unordered_map<String, DeserializeBinaryBulkStatePtr>;

    /// Call before before deserializeBinaryBulkWithMultipleStreams chain to get DeserializeBinaryBulkStatePtr.
    virtual void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & /*state*/,
        SubstreamsDeserializeStatesCache * /*cache*/) const {}

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
    virtual void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const = 0;

    /// Other variants takes a column, to avoid creating temporary Field object.
    /// Column must be non-constant.

    /// Serialize one value of a column at specified row number.
    virtual void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    /// Deserialize one value and insert into a column.
    /// If method will throw an exception, then column will be in same state as before call to method.
    virtual void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization as a literal that may be inserted into a query.
      */
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for the CSV format.
      */
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    /** Text deserialization in case when buffer contains only one value, without any escaping and delimiters.
      */
    virtual void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization intended for using in JSON format.
      */
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;
    virtual void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t /*indent*/) const
    {
        serializeTextJSON(column, row_num, ostr, settings);
    }


    /** Text serialization for putting into the XML format.
      */
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        serializeText(column, row_num, ostr, settings);
    }

    /** Text deserialization without escaping and quoting. Reads all data until first \n or \t
     *  into a temporary string and then call deserializeWholeText. It was implemented this way
     *  because this function is rarely used and because proper implementation requires a lot of
     *  additional code in data types serialization and ReadHelpers.
     */
    virtual void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;
    virtual bool tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;
    virtual void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;

    virtual void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;

    static String getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path);
    static String getFileNameForStream(const String & name_in_storage, const SubstreamPath & path);

    static String getSubcolumnNameForStream(const SubstreamPath & path);
    static String getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len);

    static void addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column);
    static ColumnPtr getFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path);

    static void addToSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path, DeserializeBinaryBulkStatePtr state);
    static DeserializeBinaryBulkStatePtr getFromSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path);

    static bool isSpecialCompressionAllowed(const SubstreamPath & path);

    static size_t getArrayLevel(const SubstreamPath & path);
    static bool hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len);
    static SubstreamData createFromPath(const SubstreamPath & path, size_t prefix_len);

    /// Returns true if subcolumn doesn't actually stores any data in column and doesn't require a separate stream
    /// for writing/reading data. For example, it's a null-map subcolumn of Variant type (it's always constructed from discriminators);.
    static bool isEphemeralSubcolumn(const SubstreamPath & path, size_t prefix_len);

    /// Returns true if stream with specified path corresponds to dynamic subcolumn.
    static bool isDynamicSubcolumn(const SubstreamPath & path, size_t prefix_len);

protected:
    template <typename State, typename StatePtr>
    State * checkAndGetState(const StatePtr & state) const;

    template <typename State, typename StatePtr>
    static State * checkAndGetState(const StatePtr & state, const ISerialization * serialization);

    [[noreturn]] void throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings &, const String & type_name) const;
};

using SerializationPtr = std::shared_ptr<const ISerialization>;
using Serializations = std::vector<SerializationPtr>;
using SerializationByName = std::unordered_map<String, SerializationPtr>;
using SubstreamType = ISerialization::Substream::Type;

template <typename State, typename StatePtr>
State * ISerialization::checkAndGetState(const StatePtr & state) const
{
    return checkAndGetState<State, StatePtr>(state, this);
}

template <typename State, typename StatePtr>
State * ISerialization::checkAndGetState(const StatePtr & state, const ISerialization * serialization)
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Got empty state for {}", demangle(typeid(*serialization).name()));

    auto * state_concrete = typeid_cast<State *>(state.get());
    if (!state_concrete)
    {
        auto & state_ref = *state;
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid State for {}. Expected: {}, got {}",
                demangle(typeid(*serialization).name()),
                demangle(typeid(State).name()),
                demangle(typeid(state_ref).name()));
    }

    return state_concrete;
}

}
