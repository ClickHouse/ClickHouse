#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

class Context;
class ReadBuffer;


/** Stream to read data in VALUES format (as in INSERT query).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      */
    ValuesRowInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_, bool interpret_expressions_);

    bool read(MutableColumns & columns) override;

private:
    ReadBuffer & istr;
    Block header;
    const Context & context;
    bool interpret_expressions;
};

}
