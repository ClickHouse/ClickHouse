#pragma once
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
class InputStreamFromASTInsertQuery : public IProfilingBlockInputStream
{
public:

	InputStreamFromASTInsertQuery(const ASTPtr & ast, ReadBuffer & input_buffer_tail_part, const BlockIO & streams, Context & context);

	Block readImpl() override			{ return res_stream->read(); }
	void readPrefixImpl() override		{ return res_stream->readPrefix(); }
	void readSuffixImpl() override		{ return res_stream->readSuffix(); }

	String getName() const override		{ return "InputStreamFromASTInsertQuery"; }
	String getID() const override		{ return "InputStreamFromASTInsertQuery(" + toString(this) + ")"; }

private:

	std::unique_ptr<ReadBuffer> input_buffer_ast_part;
	std::unique_ptr<ReadBuffer> input_buffer_contacenated;

	BlockInputStreamPtr res_stream;
};

}
