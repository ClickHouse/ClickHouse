#pragma once

#include <Parsers/IAST.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <cstddef>
#include <optional>


namespace DB
{

struct BlockIO;
class Context;

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
class InputStreamFromASTInsertQuery : public IProfilingBlockInputStream
{
public:
    InputStreamFromASTInsertQuery(const ASTPtr & ast, ReadBuffer & input_buffer_tail_part, const BlockIO & streams, Context & context);

    Block readImpl() override { return res_stream->read(); }
    void readPrefixImpl() override { return res_stream->readPrefix(); }
    void readSuffixImpl() override { return res_stream->readSuffix(); }

    String getName() const override { return "InputStreamFromASTInsertQuery"; }
    String getID() const override { return "InputStreamFromASTInsertQuery(" + toString(std::intptr_t(this)) + ")"; }

    Block getHeader() override { return res_stream->getHeader(); }

private:
    std::optional<ReadBuffer> input_buffer_ast_part;
    std::optional<ReadBuffer> input_buffer_contacenated;

    BlockInputStreamPtr res_stream;
};

}
