#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/IRowOutputStream.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{

class TemplateRowOutputStream : public IRowOutputStream
{
public:
    enum class ColumnFormat
    {
        Default,
        Escaped,
        Quoted,
        Json,
        Xml,
        Raw
    };

    TemplateRowOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings_, const String & format_template);

    void write(const Block & block, size_t row_num) override;
    void writeField(const IColumn &, const IDataType &, size_t) override {};
    void flush() override;

private:
    ColumnFormat stringToFormat(const String & format);
    void parseFormatString(const String & s, const Block & sample);
    void serializeField(const ColumnWithTypeAndName & col, size_t row_num, ColumnFormat format);

private:
    WriteBuffer & ostr;
    const FormatSettings settings;
    std::vector<String> delimiters;
    std::vector<ColumnFormat> formats;
    std::vector<size_t> format_idx_to_column_idx;
};

class TemplateBlockOutputStream : public BlockOutputStreamFromRowOutputStream
{
public:
    TemplateBlockOutputStream(RowOutputStreamPtr row_output_, const Block & header_)
        : BlockOutputStreamFromRowOutputStream(row_output_, header_) {};
    void write(const Block & block) override;
};

}
