#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <IO/PeekableReadBuffer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>

namespace DB
{

class ReadBuffer;

class SQLiteInputFormat final : public IInputFormat
{
public:

    SQLiteInputFormat(ReadBuffer & in_, const Block & header_, const RowInputFormatParams & params_,
                           const FormatSettings & format_settings_);

    String getName() const override { return "SQLiteInputFormat"; }

    void resetParser() override;
    void setReadBuffer(ReadBuffer & in_) override;

    const BlockMissingValues & getMissingValues() const override { return block_missing_values; }

private:

    using ConstantExpressionTemplates = std::vector<std::optional<ConstantExpressionTemplate>>;

    Chunk generate() override;

    const RowInputFormatParams  params;
    const FormatSettings format_settings;
    BlockMissingValues block_missing_values;
};

}
