#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    explicit DataTypeNullable(const DataTypePtr & nested_data_type_);
    std::string doGetName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    const char * getFamilyName() const override { return "Nullable"; }
    TypeIndex getTypeId() const override { return TypeIndex::Nullable; }

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

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    /** It is questionable, how NULL values could be represented in CSV. There are three variants:
      * 1. \N
      * 2. empty string (without quotes)
      * 3. NULL
      * We support all of them (however, second variant is supported by CSVRowInputStream, not by deserializeTextCSV).
      * (see also input_format_defaults_for_omitted_fields and input_format_csv_unquoted_null_literal_as_null settings)
      * In CSV, non-NULL string value, starting with \N characters, must be placed in quotes, to avoid ambiguity.
      */
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested_data_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override { return nested_data_type->shouldAlignRightInPrettyFormats(); }
    bool textCanContainOnlyValidUTF8() const override { return nested_data_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested_data_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override { return nested_data_type->canBeUsedInBooleanContext() || onlyNull(); }
    bool haveMaximumSizeOfValue() const override { return nested_data_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return 1 + nested_data_type->getMaximumSizeOfValueInMemory(); }
    bool isNullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool onlyNull() const override;
    bool canBeInsideLowCardinality() const override { return nested_data_type->canBeInsideLowCardinality(); }

    const DataTypePtr & getNestedType() const { return nested_data_type; }

    /// If ReturnType is bool, check for NULL and deserialize value into non-nullable column (and return true) or insert default value of nested type (and return false)
    /// If ReturnType is void, deserialize Nullable(T)
    template <typename ReturnType = bool>
    static ReturnType deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DataTypePtr & nested);
    template <typename ReturnType = bool>
    static ReturnType deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &, const DataTypePtr & nested);
    template <typename ReturnType = bool>
    static ReturnType deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DataTypePtr & nested);
    template <typename ReturnType = bool>
    static ReturnType deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &, const DataTypePtr & nested);

private:
    DataTypePtr nested_data_type;
};


DataTypePtr makeNullable(const DataTypePtr & type);
DataTypePtr removeNullable(const DataTypePtr & type);

}
