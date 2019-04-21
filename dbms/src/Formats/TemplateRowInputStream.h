#pragma once

#include <Core/Block.h>
#include <Formats/RowInputStreamWithDiagnosticInfo.h>
#include <Formats/FormatSettings.h>
#include <Formats/TemplateBlockOutputStream.h>
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>


namespace DB
{

class TemplateRowInputStream : public RowInputStreamWithDiagnosticInfo
{
    using ColumnFormat = ParsedTemplateFormat::ColumnFormat;
public:
    TemplateRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_, bool ignore_spaces_);

    bool read(MutableColumns & columns, RowReadExtension & extra) override;

    void readPrefix() override;

    bool allowSyncAfterError() const override;
    void syncAfterError() override;

private:
    void deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format);
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(buf); }

    bool checkForSuffix();
    void throwUnexpectedEof();
    bool compareSuffixPart(StringRef & suffix, BufferBase::Position pos, size_t available);

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                             ReadBuffer::Position & curr_pos) override;
    bool isGarbageAfterField(size_t after_col_idx, ReadBuffer::Position pos) override;
    void writeErrorStringForWrongDelimiter(WriteBuffer & out, const String & description, const String & delim);

    void skipToNextDelimiterOrEof(const String& delimeter);

private:
    PeekableReadBuffer buf;
    DataTypes data_types;

    FormatSettings settings;
    ParsedTemplateFormat format;
    ParsedTemplateFormat row_format;
    const bool ignore_spaces;
    bool synced_after_error_at_last_row = false;
};

}
