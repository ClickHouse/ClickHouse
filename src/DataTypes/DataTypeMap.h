#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>


namespace DB
{

/** Map data type.
  * Map is implemented as two arrays of keys and values.
  * Serialization of type 'Map(K, V)' is similar to serialization.
  * of 'Array(Tuple(keys K, values V))' or in other words of 'Nested(keys K, valuev V)'.
  */
class DataTypeMap final : public DataTypeWithSimpleSerialization
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    DataTypeMap(const DataTypes & elems);
    DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_);

    TypeIndex getTypeId() const override { return TypeIndex::Map; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Map"; }

    bool canBeInsideNullable() const override { return false; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;


    /** Each sub-column in a map is serialized in separate stream.
      */
    void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinaryBulkStatePrefix(
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
           DeserializeBinaryBulkSettings & settings,
           DeserializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
           const IColumn & column,
           size_t offset,
           size_t limit,
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
           IColumn & column,
           size_t limit,
           DeserializeBinaryBulkSettings & settings,
           DeserializeBinaryBulkStatePtr & state) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    bool isComparable() const override { return key_type->isComparable() && value_type->isComparable(); }
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    DataTypes getKeyValueTypes() const { return {key_type, value_type}; }

private:
    template <typename Writer>
    void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && writer) const;

    template <typename Reader>
    void deserializeTextImpl(IColumn & column, ReadBuffer & istr, bool need_safe_get_int_key, Reader && reader) const;
};

}

