#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>


namespace DB
{

/** Map data type.
  * Map(K, V) is implemented as dynamic subcolumns.
  * There's a subcolumn for each key, and one special subcolumn '_mapkeys' Array(String).
  */
class DataTypeMap final : public DataTypeWithSimpleSerialization
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeMap(const DataTypePtr & value_type_);

    TypeIndex getTypeId() const override { return TypeIndex::Map; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Map"; }

    bool canBeInsideNullable() const override { return false; }

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;

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

    void enumerateStreamsImpl(const StreamCallback & callback, SubstreamPath & path, bool sampleDynamic) const override;

    void enumerateDynamicStreams(const IColumn & column, const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinaryBulkStatePrefixImpl(
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffixImpl(
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefixImpl(
           DeserializeBinaryBulkSettings & settings,
           DeserializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreamsImpl(
           const IColumn & column,
           size_t offset,
           size_t limit,
           SerializeBinaryBulkSettings & settings,
           SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreamsImpl(
           IColumn & column,
           size_t limit,
           DeserializeBinaryBulkSettings & settings,
           DeserializeBinaryBulkStatePtr & state,
           SubstreamsCache * cache) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    bool isComparable() const override { return value_type->isComparable(); }
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    DataTypes getKeyValueTypes() const { return {key_type, value_type}; }

private:
    template <typename KeyWriter, typename ValWriter>
    void serializeTextImpl(const IColumn & column, WriteBuffer & ostr, KeyWriter && keyWriter, ValWriter valWriter) const;

    template <typename KeyReader, typename ValReader>
    void deserializeTextImpl(IColumn & column, ReadBuffer & istr, KeyReader && keyReader, ValReader && valReader) const;

    void assertKeyType() const;
};

}
