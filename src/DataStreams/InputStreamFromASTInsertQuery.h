#pragma once

#include <Parsers/IAST.h>
#include <DataStreams/IBlockInputStream.h>
#include <cstddef>
#include <memory>


namespace DB
{

struct BlockIO;
class Context;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
class InputStreamFromASTInsertQuery : public IBlockInputStream
{
public:
    InputStreamFromASTInsertQuery(
        const ASTPtr & ast,
        ReadBuffer * input_buffer_tail_part,
        const Block & header,
        const Context & context,
        const ASTPtr & input_function);

    Block readImpl() override { return res_stream->read(); }
    void readPrefixImpl() override { return res_stream->readPrefix(); }
    void readSuffixImpl() override { return res_stream->readSuffix(); }

    String getName() const override { return "InputStreamFromASTInsertQuery"; }

    Block getHeader() const override { return res_stream->getHeader(); }

private:
    std::unique_ptr<ReadBuffer> input_buffer_ast_part;
    std::unique_ptr<ReadBuffer> input_buffer_contacenated;

    BlockInputStreamPtr res_stream;
};

}
