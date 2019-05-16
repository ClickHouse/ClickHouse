#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/ConstantExpressionTemplate.h>

#include <IO/PeekableReadBuffer.h>

namespace DB
{

class Context;
class ReadBuffer;


/** Stream to read data in VALUES format (as in INSERT query).
  */
class ValuesBlockInputStream : public IBlockInputStream
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      */
    ValuesBlockInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_, const FormatSettings & format_settings, UInt64 max_block_size_);

    String getName() const override { return "ValuesBlockOutputStream"; }
    Block getHeader() const override { return header; }


    void readPrefix() override { }
    void readSuffix() override { }

private:
    typedef std::vector<std::optional<ConstantExpressionTemplate>> ConstantExpressionTemplates;

    Block readImpl() override;
    void readValue(IColumn & column, size_t column_idx, bool generate_template);

    void parseExpression(IColumn & column, size_t column_idx, bool generate_template);
    inline void assertDelimAfterValue(size_t column_idx);

private:
    PeekableReadBuffer istr;
    Block header;
    std::unique_ptr<Context> context;   /// pimpl
    const FormatSettings format_settings;
    UInt64 max_block_size;
    size_t num_columns;
    size_t total_rows = 0;

    ParserExpression parser;
    ConstantExpressionTemplates templates;
};

}
