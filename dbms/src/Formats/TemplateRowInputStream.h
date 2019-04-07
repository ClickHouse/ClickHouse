#pragma once

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/TemplateBlockOutputStream.h>
#include <IO/ReadHelpers.h>


namespace DB
{

class TemplateRowInputStream : public IRowInputStream
{
    using ColumnFormat = ParsedTemplateFormat::ColumnFormat;
public:
    TemplateRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_, bool ignore_spaces_);

    bool read(MutableColumns & columns, RowReadExtension & extra) override;

    void readPrefix() override;

    // TODO
    //bool allowSyncAfterError() const override;
    //void syncAfterError() override;
    //String getDiagnosticInfo() override;

private:
    void deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format);
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(istr); }

private:
    ReadBuffer & istr;
    Block header;
    DataTypes types;

    FormatSettings settings;
    ParsedTemplateFormat format;
    ParsedTemplateFormat row_format;
    const bool ignore_spaces;

    size_t row_count = 0;
};

}
