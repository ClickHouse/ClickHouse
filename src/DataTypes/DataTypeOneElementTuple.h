#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>

namespace DB
{

class DataTypeOneElementTuple final : public DataTypeWithSimpleSerialization
{
private:
    DataTypePtr element;
    String name;
    bool escape_delimiter;

public:
    static constexpr bool is_parametric = true;
    static constexpr auto TYPE_NAME = "__OneElementTuple";

    DataTypeOneElementTuple(const DataTypePtr & element_, const String & name_, bool escape_delimiter_ = true)
        : element(element_), name(name_), escape_delimiter(escape_delimiter_) {}

    /// Customized methods.
    const char * getFamilyName() const override { return TYPE_NAME; }
    std::string doGetName() const override;

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

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    /// Non-customized methods.
    TypeIndex getTypeId() const override { return element->getTypeId(); }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override { element->serializeBinary(field, ostr); }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override { element->deserializeBinary(field, istr); }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override { element->serializeBinary(column, row_num, ostr); }
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override { element->deserializeBinary(column, istr); }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        element->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        element->deserializeAsWholeText(column, istr, settings);
    }

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override
    {
        element->serializeProtobuf(column, row_num, protobuf, value_index);
    }

    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override
    {
        element->deserializeProtobuf(column, protobuf, allow_add_row, row_added);
    }

    bool canBeInsideNullable() const override { return element->canBeInsideNullable(); }
    MutableColumnPtr createColumn() const override { return element->createColumn(); }
    Field getDefault() const override { return element->getDefault(); }
    void insertDefaultInto(IColumn & column) const override { element->insertDefaultInto(column); }
    bool isComparable() const override { return element->isComparable(); }
    bool textCanContainOnlyValidUTF8() const override { return element->textCanContainOnlyValidUTF8(); }
    bool haveMaximumSizeOfValue() const override { return element->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return element->getMaximumSizeOfValueInMemory(); }
    size_t getSizeOfValueInMemory() const override { return element->getSizeOfValueInMemory(); }

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override { return element->tryGetSubcolumnType(subcolumn_name); }
    MutableColumnPtr getSubcolumn(const String & subcolumn_name, IColumn & column) const override { return element->getSubcolumn(subcolumn_name, column); }

private:
    void addToPath(SubstreamPath & path) const;
};

}
