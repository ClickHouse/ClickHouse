#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>


namespace DB
{

/** Map data type.
  *
  * Map's key and value only have types.
  * If only one type is set, then key's type is "String" in default.
  */
class DataTypeMap final : public DataTypeWithSimpleSerialization
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;
    DataTypePtr keys;
    DataTypePtr values;
    DataTypes kv;

public:
    static constexpr bool is_parametric = true;

    DataTypeMap(const DataTypes & elems);

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

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    const DataTypePtr & getKeyType() const { return keys; }
    const DataTypePtr & getValueType() const { return values; }
    const DataTypePtr & getVType() const { return value_type; }
    const DataTypes & getElements() const  {return kv; }
};

}

