#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>


namespace DB
{


class DataTypeArray final : public DataTypeWithSimpleSerialization
{
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    DataTypeArray(const DataTypePtr & nested_);

    TypeIndex getTypeId() const override { return TypeIndex::Array; }

    std::string doGetName() const override
    {
        return "Array(" + nested->getName() + ")";
    }

    const char * getFamilyName() const override
    {
        return "Array";
    }

    bool canBeInsideNullable() const override
    {
        return false;
    }

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

    /** Streaming serialization of arrays is arranged in a special way:
      * - elements placed in a row are written/read without array sizes;
      * - the sizes are written/read in a separate stream,
      * This is necessary, because when implementing nested structures, several arrays can have common sizes.
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

    void serializeProtobuf(const IColumn & column,
                           size_t row_num,
                           ProtobufWriter & protobuf,
                           size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column,
                             ProtobufReader & protobuf,
                             bool allow_add_row,
                             bool & row_added) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested->cannotBeStoredInTables(); }
    bool textCanContainOnlyValidUTF8() const override { return nested->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested->canBeComparedWithCollation(); }

    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override
    {
        return nested->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }

    const DataTypePtr & getNestedType() const { return nested; }

    /// 1 for plain array, 2 for array of arrays and so on.
    size_t getNumberOfDimensions() const;
};

}
