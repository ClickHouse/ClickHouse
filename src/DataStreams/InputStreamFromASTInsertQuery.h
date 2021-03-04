#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <IO/ConcatReadBuffer.h>
#include <Parsers/IAST.h>

#include <cstddef>
#include <memory>


namespace DB
{

class Context;

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
class InputStreamFromASTInsertQuery : public IBlockInputStream
{
public:
    InputStreamFromASTInsertQuery(const ASTPtr & ast, const Block & header, const Context & context, const ASTPtr & input_function);
    InputStreamFromASTInsertQuery(
        const ASTPtr & ast, ReadBuffer & tail, const Block & header, const Context & context, const ASTPtr & input_function);

    Block readImpl() override { return res_stream->read(); }
    void readPrefixImpl() override { return res_stream->readPrefix(); }
    void readSuffixImpl() override { return res_stream->readSuffix(); }

    String getName() const override { return "InputStreamFromASTInsertQuery"; }

    Block getHeader() const override { return res_stream->getHeader(); }

    void appendBuffer(std::unique_ptr<ReadBuffer> buffer) { input_buffer.appendBuffer(std::move(buffer)); }

private:
    ConcatReadBuffer input_buffer;

    BlockInputStreamPtr res_stream;
};

}
