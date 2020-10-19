#pragma once

#include <memory>
#include <Common/COW.h>
#include <boost/noncopyable.hpp>
#include <DataTypes/DataTypeCustom.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

class IDataType;
struct FormatSettings;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class Field;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

class ProtobufReader;
class ProtobufWriter;


/** Properties of data type.
  * Contains methods for serialization/deserialization.
  * Implementations of this interface represent a data type (example: UInt8)
  *  or parametric family of data types (example: Array(...)).
  *
  * DataType is totally immutable object. You can always share them.
  */
class IDataType : private boost::noncopyable
{
public:
    IDataType();
    virtual ~IDataType();

    /// Compile time flag. If false, then if C++ types are the same, then SQL types are also the same.
    /// Example: DataTypeString is not parametric: thus all instances of DataTypeString are the same SQL type.
    /// Example: DataTypeFixedString is parametric: different instances of DataTypeFixedString may be different SQL types.
    /// Place it in descendants:
    /// static constexpr bool is_parametric = false;

    /// Name of data type (examples: UInt64, Array(String)).
    String getName() const;

    /// Name of data type family (example: FixedString, Array).
    virtual const char * getFamilyName() const = 0;

    /// Data type id. It's used for runtime type checks.
    virtual TypeIndex getTypeId() const = 0;

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
      * For multidimensional arrays, path can have arbiraty length.
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
        };
        Type type;

        /// Index of tuple element, starting at 1.
        String tuple_element_name;

        Substream(Type type_) : type(type_) {}
    };

    using SubstreamPath = std::vector<Substream>;

    using StreamCallback = std::function<void(const SubstreamPath &)>;
    virtual void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
    {
        callback(path);
    }
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
        SerializeBinaryBulkStatePtr & /*state*/) const
    {
        if (WriteBuffer * stream = settings.getter(settings.path))
            serializeBinaryBulk(column, *stream, offset, limit);
    }

    /// Read no more than limit values and append them into column.
    virtual void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & /*state*/) const
    {
        if (ReadBuffer * stream = settings.getter(settings.path))
            deserializeBinaryBulk(column, *stream, limit, settings.avg_value_size_hint);
    }

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

    /** Serialize to a protobuf. */
    virtual void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const = 0;
    virtual void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    void serializeAsTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const;

    void deserializeAsTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization as a literal that may be inserted into a query.
      */
    void serializeAsTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const;

    void deserializeAsTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for the CSV format.
      */
    void serializeAsTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const;
    void deserializeAsTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    void serializeAsText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const;

    /** Text deserialization in case when buffer contains only one value, without any escaping and delimiters.
      */
    void deserializeAsWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization intended for using in JSON format.
      */
    void serializeAsTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const;
    void deserializeAsTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for putting into the XML format.
      */
    void serializeAsTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;

protected:
    virtual String doGetName() const;

    /// Default implementations of text serialization in case of 'custom_text_serialization' is not set.

    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        serializeText(column, row_num, ostr, settings);
    }

public:
    /** Create empty column for corresponding type.
      */
    virtual MutableColumnPtr createColumn() const = 0;

    /** Create ColumnConst for corresponding type, with specified size and value.
      */
    ColumnPtr createColumnConst(size_t size, const Field & field) const;
    ColumnPtr createColumnConstWithDefaultValue(size_t size) const;

    /** Get default value of data type.
      * It is the "default" default, regardless the fact that a table could contain different user-specified default.
      */
    virtual Field getDefault() const = 0;

    /** The data type can be promoted in order to try to avoid overflows.
      * Data types which can be promoted are typically Number or Decimal data types.
      */
    virtual bool canBePromoted() const { return false; }

    /** Return the promoted numeric data type of the current data type. Throw an exception if `canBePromoted() == false`.
      */
    virtual DataTypePtr promoteNumericType() const;

    /** Directly insert default value into a column. Default implementation use method IColumn::insertDefault.
      * This should be overriden if data type default value differs from column default value (example: Enum data types).
      */
    virtual void insertDefaultInto(IColumn & column) const;

    /// Checks that two instances belong to the same type
    virtual bool equals(const IDataType & rhs) const = 0;


    /// Various properties on behaviour of data type.

    /** The data type is dependent on parameters and types with different parameters are different.
      * Examples: FixedString(N), Tuple(T1, T2), Nullable(T).
      * Otherwise all instances of the same class are the same types.
      */
    virtual bool isParametric() const = 0;

    /** The data type is dependent on parameters and at least one of them is another type.
      * Examples: Tuple(T1, T2), Nullable(T). But FixedString(N) is not.
      */
    virtual bool haveSubtypes() const = 0;

    /** Can appear in table definition.
      * Counterexamples: Interval, Nothing.
      */
    virtual bool cannotBeStoredInTables() const { return false; }

    /** In text formats that render "pretty" tables,
      *  is it better to align value right in table cell.
      * Examples: numbers, even nullable.
      */
    virtual bool shouldAlignRightInPrettyFormats() const { return false; }

    /** Does formatted value in any text format can contain anything but valid UTF8 sequences.
      * Example: String (because it can contain arbitrary bytes).
      * Counterexamples: numbers, Date, DateTime.
      * For Enum, it depends.
      */
    virtual bool textCanContainOnlyValidUTF8() const { return false; }

    /** Is it possible to compare for less/greater, to calculate min/max?
      * Not necessarily totally comparable. For example, floats are comparable despite the fact that NaNs compares to nothing.
      * The same for nullable of comparable types: they are comparable (but not totally-comparable).
      */
    virtual bool isComparable() const { return false; }

    /** Does it make sense to use this type with COLLATE modifier in ORDER BY.
      * Example: String, but not FixedString.
      */
    virtual bool canBeComparedWithCollation() const { return false; }

    /** If the type is totally comparable (Ints, Date, DateTime, not nullable, not floats)
      *  and "simple" enough (not String, FixedString) to be used as version number
      *  (to select rows with maximum version).
      */
    virtual bool canBeUsedAsVersion() const { return false; }

    /** Values of data type can be summed (possibly with overflow, within the same data type).
      * Example: numbers, even nullable. Not Date/DateTime. Not Enum.
      * Enums can be passed to aggregate function 'sum', but the result is Int64, not Enum, so they are not summable.
      */
    virtual bool isSummable() const { return false; }

    /** Can be used in operations like bit and, bit shift, bit not, etc.
      */
    virtual bool canBeUsedInBitOperations() const { return false; }

    /** Can be used in boolean context (WHERE, HAVING).
      * UInt8, maybe nullable.
      */
    virtual bool canBeUsedInBooleanContext() const { return false; }

    /** Numbers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByNumber() const { return false; }

    /** Integers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByInteger() const { return false; }

    /** Unsigned Integers, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByUnsignedInteger() const { return false; }

    /** Values are unambiguously identified by contents of contiguous memory region,
      *  that can be obtained by IColumn::getDataAt method.
      * Examples: numbers, Date, DateTime, String, FixedString,
      *  and Arrays of numbers, Date, DateTime, FixedString, Enum, but not String.
      *  (because Array(String) values became ambiguous if you concatenate Strings).
      * Counterexamples: Nullable, Tuple.
      */
    virtual bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const { return false; }

    virtual bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const
    {
        return isValueRepresentedByNumber();
    }

    /** Example: numbers, Date, DateTime, FixedString, Enum... Nullable and Tuple of such types.
      * Counterexamples: String, Array.
      * It's Ok to return false for AggregateFunction despite the fact that some of them have fixed size state.
      */
    virtual bool haveMaximumSizeOfValue() const { return false; }

    /** Size in amount of bytes in memory. Throws an exception if not haveMaximumSizeOfValue.
      */
    virtual size_t getMaximumSizeOfValueInMemory() const { return getSizeOfValueInMemory(); }

    /** Throws an exception if value is not of fixed size.
      */
    virtual size_t getSizeOfValueInMemory() const;

    /** Integers (not floats), Enum, String, FixedString.
      */
    virtual bool isCategorial() const { return false; }

    virtual bool isNullable() const { return false; }

    /** Is this type can represent only NULL value? (It also implies isNullable)
      */
    virtual bool onlyNull() const { return false; }

    /** If this data type cannot be wrapped in Nullable data type.
      */
    virtual bool canBeInsideNullable() const { return false; }

    virtual bool lowCardinality() const { return false; }

    /// Strings, Numbers, Date, DateTime, Nullable
    virtual bool canBeInsideLowCardinality() const { return false; }

    /// Updates avg_value_size_hint for newly read column. Uses to optimize deserialization. Zero expected for first column.
    static void updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint);

    static String getFileNameForStream(const String & column_name, const SubstreamPath & path);

private:
    friend class DataTypeFactory;
    /// Customize this DataType
    void setCustomization(DataTypeCustomDescPtr custom_desc_) const;

    /// This is mutable to allow setting custom name and serialization on `const IDataType` post construction.
    mutable DataTypeCustomNamePtr custom_name;
    mutable DataTypeCustomTextSerializationPtr custom_text_serialization;

public:
    const IDataTypeCustomName * getCustomName() const { return custom_name.get(); }
};


/// Some sugar to check data type of IDataType
struct WhichDataType
{
    TypeIndex idx;

    WhichDataType(TypeIndex idx_ = TypeIndex::Nothing)
        : idx(idx_)
    {}

    WhichDataType(const IDataType & data_type)
        : idx(data_type.getTypeId())
    {}

    WhichDataType(const IDataType * data_type)
        : idx(data_type->getTypeId())
    {}

    WhichDataType(const DataTypePtr & data_type)
        : idx(data_type->getTypeId())
    {}

    bool isUInt8() const { return idx == TypeIndex::UInt8; }
    bool isUInt16() const { return idx == TypeIndex::UInt16; }
    bool isUInt32() const { return idx == TypeIndex::UInt32; }
    bool isUInt64() const { return idx == TypeIndex::UInt64; }
    bool isUInt128() const { return idx == TypeIndex::UInt128; }
    bool isUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64() || isUInt128(); }
    bool isNativeUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64(); }

    bool isInt8() const { return idx == TypeIndex::Int8; }
    bool isInt16() const { return idx == TypeIndex::Int16; }
    bool isInt32() const { return idx == TypeIndex::Int32; }
    bool isInt64() const { return idx == TypeIndex::Int64; }
    bool isInt128() const { return idx == TypeIndex::Int128; }
    bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64() || isInt128(); }
    bool isNativeInt() const { return isInt8() || isInt16() || isInt32() || isInt64(); }

    bool isDecimal32() const { return idx == TypeIndex::Decimal32; }
    bool isDecimal64() const { return idx == TypeIndex::Decimal64; }
    bool isDecimal128() const { return idx == TypeIndex::Decimal128; }
    bool isDecimal() const { return isDecimal32() || isDecimal64() || isDecimal128(); }

    bool isFloat32() const { return idx == TypeIndex::Float32; }
    bool isFloat64() const { return idx == TypeIndex::Float64; }
    bool isFloat() const { return isFloat32() || isFloat64(); }

    bool isEnum8() const { return idx == TypeIndex::Enum8; }
    bool isEnum16() const { return idx == TypeIndex::Enum16; }
    bool isEnum() const { return isEnum8() || isEnum16(); }

    bool isDate() const { return idx == TypeIndex::Date; }
    bool isDateTime() const { return idx == TypeIndex::DateTime; }
    bool isDateTime64() const { return idx == TypeIndex::DateTime64; }
    bool isDateOrDateTime() const { return isDate() || isDateTime() || isDateTime64(); }

    bool isString() const { return idx == TypeIndex::String; }
    bool isFixedString() const { return idx == TypeIndex::FixedString; }
    bool isStringOrFixedString() const { return isString() || isFixedString(); }

    bool isUUID() const { return idx == TypeIndex::UUID; }
    bool isArray() const { return idx == TypeIndex::Array; }
    bool isTuple() const { return idx == TypeIndex::Tuple; }
    bool isSet() const { return idx == TypeIndex::Set; }
    bool isInterval() const { return idx == TypeIndex::Interval; }

    bool isNothing() const { return idx == TypeIndex::Nothing; }
    bool isNullable() const { return idx == TypeIndex::Nullable; }
    bool isFunction() const { return idx == TypeIndex::Function; }
    bool isAggregateFunction() const { return idx == TypeIndex::AggregateFunction; }
};

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

template <typename T>
inline bool isDate(const T & data_type) { return WhichDataType(data_type).isDate(); }
template <typename T>
inline bool isDateOrDateTime(const T & data_type) { return WhichDataType(data_type).isDateOrDateTime(); }
template <typename T>
inline bool isDateTime(const T & data_type) { return WhichDataType(data_type).isDateTime(); }
template <typename T>
inline bool isDateTime64(const T & data_type) { return WhichDataType(data_type).isDateTime64(); }

inline bool isEnum(const DataTypePtr & data_type) { return WhichDataType(data_type).isEnum(); }
inline bool isDecimal(const DataTypePtr & data_type) { return WhichDataType(data_type).isDecimal(); }
inline bool isTuple(const DataTypePtr & data_type) { return WhichDataType(data_type).isTuple(); }
inline bool isArray(const DataTypePtr & data_type) { return WhichDataType(data_type).isArray(); }

template <typename T>
inline bool isUInt8(const T & data_type)
{
    return WhichDataType(data_type).isUInt8();
}

template <typename T>
inline bool isUnsignedInteger(const T & data_type)
{
    return WhichDataType(data_type).isUInt();
}

template <typename T>
inline bool isInteger(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

template <typename T>
inline bool isFloat(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isFloat();
}

template <typename T>
inline bool isNativeInteger(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt();
}


template <typename T>
inline bool isNativeNumber(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt() || which.isFloat();
}

template <typename T>
inline bool isNumber(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDecimal();
}

template <typename T>
inline bool isColumnedAsNumber(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDateOrDateTime() || which.isUUID();
}

template <typename T>
inline bool isColumnedAsDecimal(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isDecimal() || which.isDateTime64();
}

template <typename T>
inline bool isString(const T & data_type)
{
    return WhichDataType(data_type).isString();
}

template <typename T>
inline bool isFixedString(const T & data_type)
{
    return WhichDataType(data_type).isFixedString();
}

template <typename T>
inline bool isStringOrFixedString(const T & data_type)
{
    return WhichDataType(data_type).isStringOrFixedString();
}

template <typename T>
inline bool isNotCreatable(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isNothing() || which.isFunction() || which.isSet();
}

inline bool isNotDecimalButComparableToDecimal(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

inline bool isCompilableType(const DataTypePtr & data_type)
{
    return data_type->isValueRepresentedByNumber() && !isDecimal(data_type);
}

template <TypeIndex TYPE_IDX, typename DataType>
inline bool isDataType(const DataType & data_type)
{
    WhichDataType which(data_type);
    return which.idx == TYPE_IDX;
}

template <typename ExpectedDataType, typename DataType>
inline bool isDataType(const DataType & data_type)
{
    return isDataType<ExpectedDataType::type_id>(data_type);
}

template <typename DataType> constexpr bool IsDataTypeDecimal = false;
template <typename DataType> constexpr bool IsDataTypeNumber = false;
template <typename DataType> constexpr bool IsDataTypeDateOrDateTime = false;

template <typename DataType> constexpr bool IsDataTypeDecimalOrNumber = IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <typename T>
class DataTypeDecimal;

template <typename T>
class DataTypeNumber;

class DataTypeDate;
class DataTypeDateTime;
class DataTypeDateTime64;

template <typename T> constexpr bool IsDataTypeDecimal<DataTypeDecimal<T>> = true;
template <> inline constexpr bool IsDataTypeDecimal<DataTypeDateTime64> = true;

template <typename T> constexpr bool IsDataTypeNumber<DataTypeNumber<T>> = true;

template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDateTime> = true;
template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDateTime64> = true;

}

