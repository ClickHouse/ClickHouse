#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class Context;
class ReadBuffer;


/** Stream to read data in VALUES format (as in INSERT query).
  */
class ValuesRowInputFormat : public IRowInputFormat
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      */
    ValuesRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const Context & context_, const FormatSettings & format_settings_);

    String getName() const override { return "ValuesRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;

private:
    std::unique_ptr<Context> context;   /// pimpl
    const FormatSettings format_settings;
};

}
