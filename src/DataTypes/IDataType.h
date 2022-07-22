#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include <Core/Names.h>
#include <Core/TypeId.h>
#include <Common/COW.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>


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

struct NameAndTypePair;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = std::vector<DataTypeWithConstInfo>;

/** Properties of data type.
  *
  * Contains methods for getting serialization instances.
  * One data type may have different serializations, which can be chosen
  * dynamically before reading or writing, according to information about
  * column content (see `getSerialization` methods).
  *
  * Implementations of this interface represent a data type (example: UInt8)
  *  or parametric family of data types (example: Array(...)).
  *
  * DataType is totally immutable object. You can always share them.
  */
class IDataType : private boost::noncopyable, public std::enable_shared_from_this<IDataType>
{
public:
    IDataType() = default;
    virtual ~IDataType();

    /// Compile time flag. If false, then if C++ types are the same, then SQL types are also the same.
    /// Example: DataTypeString is not parametric: thus all instances of DataTypeString are the same SQL type.
    /// Example: DataTypeFixedString is parametric: different instances of DataTypeFixedString may be different SQL types.
    /// Place it in descendants:
    /// static constexpr bool is_parametric = false;

    /// Name of data type (examples: UInt64, Array(String)).
    String getName() const
    {
      if (custom_name)
          return custom_name->getName();
      else
          return doGetName();
    }

    DataTypePtr getPtr() const { return shared_from_this(); }

    /// Name of data type family (example: FixedString, Array).
    virtual const char * getFamilyName() const = 0;

    /// Data type id. It's used for runtime type checks.
    virtual TypeIndex getTypeId() const = 0;

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const;
    DataTypePtr getSubcolumnType(const String & subcolumn_name) const;

    ColumnPtr tryGetSubcolumn(const String & subcolumn_name, const ColumnPtr & column) const;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const ColumnPtr & column) const;

    SerializationPtr getSubcolumnSerialization(const String & subcolumn_name, const SerializationPtr & serialization) const;

    using SubstreamData = ISerialization::SubstreamData;
    using SubstreamPath = ISerialization::SubstreamPath;

    using SubcolumnCallback = std::function<void(
        const SubstreamPath &,
        const String &,
        const SubstreamData &)>;

    static void forEachSubcolumn(
        const SubcolumnCallback & callback,
        const SubstreamData & data);

    Names getSubcolumnNames() const;

    virtual MutableSerializationInfoPtr createSerializationInfo(
        const SerializationInfo::Settings & settings) const;

    /// TODO: support more types.
    virtual bool supportsSparseSerialization() const { return !haveSubtypes(); }

    SerializationPtr getDefaultSerialization() const;
    SerializationPtr getSparseSerialization() const;

    /// Chooses serialization according to serialization kind.
    SerializationPtr getSerialization(ISerialization::Kind kind) const;

    /// Chooses serialization according to collected information about content of column.
    virtual SerializationPtr getSerialization(const SerializationInfo & info) const;

    /// Chooses between subcolumn serialization and regular serialization according to @column.
    /// This method typically should be used to get serialization for reading column or subcolumn.
    static SerializationPtr getSerialization(const NameAndTypePair & column, const SerializationInfo & info);

    static SerializationPtr getSerialization(const NameAndTypePair & column);

protected:
    virtual String doGetName() const { return getFamilyName(); }
    virtual SerializationPtr doGetDefaultSerialization() const = 0;

public:
    /** Create empty column for corresponding type and default serialization.
      */
    virtual MutableColumnPtr createColumn() const = 0;

    /** Create empty column for corresponding type and serialization.
     */
    virtual MutableColumnPtr createColumn(const ISerialization & serialization) const;

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
      * This should be overridden if data type default value differs from column default value (example: Enum data types).
      */
    virtual void insertDefaultInto(IColumn & column) const;

    void insertManyDefaultsInto(IColumn & column, size_t n) const;

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

    /** If the type is totally comparable (Ints, Date, DateTime, DateTime64, not nullable, not floats)
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

    /// Checks if this type is LowCardinality(Nullable(...))
    virtual bool isLowCardinalityNullable() const { return false; }

    /// Strings, Numbers, Date, DateTime, Nullable
    virtual bool canBeInsideLowCardinality() const { return false; }

    /// Updates avg_value_size_hint for newly read column. Uses to optimize deserialization. Zero expected for first column.
    static void updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint);

protected:
    friend class DataTypeFactory;
    friend class AggregateFunctionSimpleState;

    /// Customize this DataType
    void setCustomization(DataTypeCustomDescPtr custom_desc_) const;

    /// This is mutable to allow setting custom name and serialization on `const IDataType` post construction.
    mutable DataTypeCustomNamePtr custom_name;
    mutable SerializationPtr custom_serialization;

public:
    const IDataTypeCustomName * getCustomName() const { return custom_name.get(); }
    const ISerialization * getCustomSerialization() const { return custom_serialization.get(); }

private:
    template <typename Ptr>
    Ptr getForSubcolumn(
        const String & subcolumn_name,
        const SubstreamData & data,
        Ptr SubstreamData::*member,
        bool throw_if_null) const;
};


/// Some sugar to check data type of IDataType
struct WhichDataType
{
    TypeIndex idx;

    constexpr WhichDataType(TypeIndex idx_ = TypeIndex::Nothing) : idx(idx_) {} /// NOLINT
    constexpr WhichDataType(const IDataType & data_type) : idx(data_type.getTypeId()) {} /// NOLINT
    constexpr WhichDataType(const IDataType * data_type) : idx(data_type->getTypeId()) {} /// NOLINT

    // shared ptr -> is non-constexpr in gcc
    WhichDataType(const DataTypePtr & data_type) : idx(data_type->getTypeId()) {} /// NOLINT

    constexpr bool isUInt8() const { return idx == TypeIndex::UInt8; }
    constexpr bool isUInt16() const { return idx == TypeIndex::UInt16; }
    constexpr bool isUInt32() const { return idx == TypeIndex::UInt32; }
    constexpr bool isUInt64() const { return idx == TypeIndex::UInt64; }
    constexpr bool isUInt128() const { return idx == TypeIndex::UInt128; }
    constexpr bool isUInt256() const { return idx == TypeIndex::UInt256; }
    constexpr bool isUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64() || isUInt128() || isUInt256(); }
    constexpr bool isNativeUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64(); }

    constexpr bool isInt8() const { return idx == TypeIndex::Int8; }
    constexpr bool isInt16() const { return idx == TypeIndex::Int16; }
    constexpr bool isInt32() const { return idx == TypeIndex::Int32; }
    constexpr bool isInt64() const { return idx == TypeIndex::Int64; }
    constexpr bool isInt128() const { return idx == TypeIndex::Int128; }
    constexpr bool isInt256() const { return idx == TypeIndex::Int256; }
    constexpr bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64() || isInt128() || isInt256(); }
    constexpr bool isNativeInt() const { return isInt8() || isInt16() || isInt32() || isInt64(); }

    constexpr bool isDecimal32() const { return idx == TypeIndex::Decimal32; }
    constexpr bool isDecimal64() const { return idx == TypeIndex::Decimal64; }
    constexpr bool isDecimal128() const { return idx == TypeIndex::Decimal128; }
    constexpr bool isDecimal256() const { return idx == TypeIndex::Decimal256; }
    constexpr bool isDecimal() const { return isDecimal32() || isDecimal64() || isDecimal128() || isDecimal256(); }

    constexpr bool isFloat32() const { return idx == TypeIndex::Float32; }
    constexpr bool isFloat64() const { return idx == TypeIndex::Float64; }
    constexpr bool isFloat() const { return isFloat32() || isFloat64(); }

    constexpr bool isEnum8() const { return idx == TypeIndex::Enum8; }
    constexpr bool isEnum16() const { return idx == TypeIndex::Enum16; }
    constexpr bool isEnum() const { return isEnum8() || isEnum16(); }

    constexpr bool isDate() const { return idx == TypeIndex::Date; }
    constexpr bool isDate32() const { return idx == TypeIndex::Date32; }
    constexpr bool isDateTime() const { return idx == TypeIndex::DateTime; }
    constexpr bool isDateTime64() const { return idx == TypeIndex::DateTime64; }
    constexpr bool isDateOrDate32() const { return isDate() || isDate32(); }

    constexpr bool isString() const { return idx == TypeIndex::String; }
    constexpr bool isFixedString() const { return idx == TypeIndex::FixedString; }
    constexpr bool isStringOrFixedString() const { return isString() || isFixedString(); }

    constexpr bool isUUID() const { return idx == TypeIndex::UUID; }
    constexpr bool isArray() const { return idx == TypeIndex::Array; }
    constexpr bool isTuple() const { return idx == TypeIndex::Tuple; }
    constexpr bool isMap() const {return idx == TypeIndex::Map; }
    constexpr bool isSet() const { return idx == TypeIndex::Set; }
    constexpr bool isInterval() const { return idx == TypeIndex::Interval; }
    constexpr bool isObject() const { return idx == TypeIndex::Object; }

    constexpr bool isNothing() const { return idx == TypeIndex::Nothing; }
    constexpr bool isNullable() const { return idx == TypeIndex::Nullable; }
    constexpr bool isFunction() const { return idx == TypeIndex::Function; }
    constexpr bool isAggregateFunction() const { return idx == TypeIndex::AggregateFunction; }
    constexpr bool isSimple() const  { return isInt() || isUInt() || isFloat() || isString(); }

    constexpr bool isLowCarnality() const { return idx == TypeIndex::LowCardinality; }
};

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

template <typename T>
inline bool isDate(const T & data_type) { return WhichDataType(data_type).isDate(); }
template <typename T>
inline bool isDate32(const T & data_type) { return WhichDataType(data_type).isDate32(); }
template <typename T>
inline bool isDateOrDate32(const T & data_type) { return WhichDataType(data_type).isDateOrDate32(); }
template <typename T>
inline bool isDateTime(const T & data_type) { return WhichDataType(data_type).isDateTime(); }
template <typename T>
inline bool isDateTime64(const T & data_type) { return WhichDataType(data_type).isDateTime64(); }

inline bool isEnum(const DataTypePtr & data_type) { return WhichDataType(data_type).isEnum(); }
inline bool isDecimal(const DataTypePtr & data_type) { return WhichDataType(data_type).isDecimal(); }
inline bool isTuple(const DataTypePtr & data_type) { return WhichDataType(data_type).isTuple(); }
inline bool isArray(const DataTypePtr & data_type) { return WhichDataType(data_type).isArray(); }
inline bool isMap(const DataTypePtr & data_type) {return WhichDataType(data_type).isMap(); }
inline bool isNothing(const DataTypePtr & data_type) { return WhichDataType(data_type).isNothing(); }
inline bool isUUID(const DataTypePtr & data_type) { return WhichDataType(data_type).isUUID(); }

template <typename T>
inline bool isObject(const T & data_type)
{
    return WhichDataType(data_type).isObject();
}

template <typename T>
inline bool isUInt8(const T & data_type)
{
    return WhichDataType(data_type).isUInt8();
}

template <typename T>
inline bool isUInt64(const T & data_type)
{
    return WhichDataType(data_type).isUInt64();
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
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64() || which.isUUID();
}

template <typename T>
inline bool isColumnedAsDecimal(const T & data_type)
{
    WhichDataType which(data_type);
    return which.isDecimal() || which.isDateTime64();
}

// Same as isColumnedAsDecimal but also checks value type of underlyig column.
template <typename T, typename DataType>
inline bool isColumnedAsDecimalT(const DataType & data_type)
{
    const WhichDataType which(data_type);
    return (which.isDecimal() || which.isDateTime64()) && which.idx == TypeToTypeIndex<T>;
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
    return which.isInt() || which.isUInt() || which.isFloat();
}

inline bool isCompilableType(const DataTypePtr & data_type)
{
    return data_type->isValueRepresentedByNumber() && !isDecimal(data_type);
}

inline bool isBool(const DataTypePtr & data_type)
{
    return data_type->getName() == "Bool";
}

inline bool isAggregateFunction(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);
    return which.isAggregateFunction();
}

template <typename DataType> constexpr bool IsDataTypeDecimal = false;
template <typename DataType> constexpr bool IsDataTypeNumber = false;
template <typename DataType> constexpr bool IsDataTypeDateOrDateTime = false;
template <typename DataType> constexpr bool IsDataTypeEnum = false;

template <typename DataType> constexpr bool IsDataTypeDecimalOrNumber = IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <is_decimal T>
class DataTypeDecimal;

template <typename T>
class DataTypeNumber;

class DataTypeDate;
class DataTypeDate32;
class DataTypeDateTime;
class DataTypeDateTime64;

template <is_decimal T> constexpr bool IsDataTypeDecimal<DataTypeDecimal<T>> = true;
template <> inline constexpr bool IsDataTypeDecimal<DataTypeDateTime64> = true;

template <typename T> constexpr bool IsDataTypeNumber<DataTypeNumber<T>> = true;

template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDate32> = true;
template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDateTime> = true;
template <> inline constexpr bool IsDataTypeDateOrDateTime<DataTypeDateTime64> = true;

template <typename T>
class DataTypeEnum;

template <typename T> inline constexpr bool IsDataTypeEnum<DataTypeEnum<T>> = true;

#define FOR_BASIC_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)

#define FOR_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256) \
    M(Float32) \
    M(Float64)
}
