#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>

namespace DB
{

class SerializationUserDefinedType final : public SimpleTextSerialization {
private:
    SerializationPtr nested;
    ASTPtr nested_ast;
    ASTPtr input;
    ASTPtr output;
    SerializationPtr string_serialization;
    ContextPtr context;

public:
    SerializationUserDefinedType(
        const SerializationPtr & nested_,
        const ASTPtr & nested_ast_,
        const ASTPtr & input_,
        const ASTPtr & output_,
        ContextPtr context_);

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
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
private:
    ColumnPtr convertToStringColumn(const IColumn & source_column) const;

    ColumnPtr convertFromStringColumn(std::function<void(MutableColumnPtr &)> string_deserializator) const;

    ColumnPtr executeFunction(ASTPtr function_core, const ColumnsWithTypeAndName & arguments) const;
};

}
