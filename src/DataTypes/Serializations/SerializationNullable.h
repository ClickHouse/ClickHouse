#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class SerializationNullable : public ISerialization
{
private:
    SerializationPtr nested;

public:
    explicit SerializationNullable(const SerializationPtr & nested_) : nested(nested_) {}

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
            const IColumn & column,
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
            DeserializeBinaryBulkSettings & settings,
            DeserializeBinaryBulkStatePtr & state,
            SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
            const IColumn & column,
            size_t offset,
            size_t limit,
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
            ColumnPtr & column,
            size_t limit,
            DeserializeBinaryBulkSettings & settings,
            DeserializeBinaryBulkStatePtr & state,
            SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    /** It is questionable, how NULL values could be represented in CSV. There are three variants:
      * 1. \N
      * 2. empty string (without quotes)
      * 3. NULL
      * We support all of them (however, second variant is supported by CSVRowInputFormat, not by deserializeTextCSV).
      * (see also input_format_defaults_for_omitted_fields and input_format_csv_unquoted_null_literal_as_null settings)
      * In CSV, non-NULL string value, starting with \N characters, must be placed in quotes, to avoid ambiguity.
      */
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    /// If Check for NULL and deserialize value into non-nullable column (and return true) or insert default value of nested type (and return false)
    static bool deserializeNullAsDefaultOrNestedWholeText(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool deserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool deserializeNullAsDefaultOrNestedTextQuoted(IColumn & nested_column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested_serialization);
    static bool deserializeNullAsDefaultOrNestedTextCSV(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool deserializeNullAsDefaultOrNestedTextJSON(IColumn & nested_column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested_serialization);
    static bool deserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);

    /// If Check for NULL and deserialize value into non-nullable column or insert default value of nested type.
    /// Return true if parsing was successful and false in case of any error.
    static bool tryDeserializeNullAsDefaultOrNestedWholeText(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool tryDeserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool tryDeserializeNullAsDefaultOrNestedTextQuoted(IColumn & nested_column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested_serialization);
    static bool tryDeserializeNullAsDefaultOrNestedTextCSV(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);
    static bool tryDeserializeNullAsDefaultOrNestedTextJSON(IColumn & nested_column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested_serialization);
    static bool tryDeserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization);


    static void serializeNullEscaped(WriteBuffer & ostr, const FormatSettings & settings);
    static bool tryDeserializeNullEscaped(ReadBuffer & istr, const FormatSettings & settings);
    static void serializeNullQuoted(WriteBuffer & ostr);
    static bool tryDeserializeNullQuoted(ReadBuffer & istr);
    static void serializeNullCSV(WriteBuffer & ostr, const FormatSettings & settings);
    static bool tryDeserializeNullCSV(ReadBuffer & istr, const FormatSettings & settings);
    static void serializeNullJSON(WriteBuffer & ostr);
    static bool tryDeserializeNullJSON(ReadBuffer & istr);
    static void serializeNullRaw(WriteBuffer & ostr, const FormatSettings & settings);
    static bool tryDeserializeNullRaw(ReadBuffer & istr, const FormatSettings & settings);
    static void serializeNullText(WriteBuffer & ostr, const FormatSettings & settings);
    static bool tryDeserializeNullText(ReadBuffer & istr);
    static void serializeNullXML(WriteBuffer & ostr);

private:
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr null_map;

        explicit SubcolumnCreator(const ColumnPtr & null_map_) : null_map(null_map_) {}

        DataTypePtr create(const DataTypePtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };
};

}
