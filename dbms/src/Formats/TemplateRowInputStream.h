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

    // TODO
    //bool allowSyncAfterError() const override;
    //void syncAfterError() override;

private:
    void deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format);
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(buf); }
    bool checkForSuffix();
    bool compareSuffixPart(StringRef & suffix, BufferBase::Position pos, size_t available);

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out,
                                                size_t max_length_of_column_name, size_t max_length_of_data_type_name) override;
    void tryDeserializeFiled(MutableColumns & columns, size_t col_idx,
                                     ReadBuffer::Position & prev_pos, ReadBuffer::Position & curr_pos) override;
    bool isGarbageAfterField(size_t after_col_idx, ReadBuffer::Position pos) override;
    void writeErrorStringForWrongDelimiter(WriteBuffer & out, const String & description, const String & delim);


private:
    PeekableReadBuffer buf;

    FormatSettings settings;
    ParsedTemplateFormat format;
    ParsedTemplateFormat row_format;
    const bool ignore_spaces;
};

}
