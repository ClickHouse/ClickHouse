#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class SerializationUserDefined : public ISerialization
{
private:
    SerializationPtr nested_serialization;
    DataTypePtr base_data_type;
    String input_function_name;
    String output_function_name;
    ContextPtr context;

    ColumnPtr executeFunction(const String & function_name, const Field & field) const;
    ColumnPtr executeFunction(const String & function_name, const IColumn & current_column, size_t row_num_for_single_row_column) const;
    ColumnPtr executeFunction(const String & function_name, ColumnPtr & current_column) const;

public:
    SerializationUserDefined(
        const SerializationPtr & nested_serialization_,
        DataTypePtr base_data_type_,
        const String & input_function_name_,
        const String & output_function_name_,
        ContextPtr context_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &settings) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

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
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    bool isUserDefined() const { return true; }
    DataTypePtr getBaseDataType() const { return base_data_type; }
};

}
