#pragma once
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


class InputStreamFromASTInsertQuery : public IProfilingBlockInputStream
{
public:

	InputStreamFromASTInsertQuery(const ASTPtr & ast, ReadBuffer & istr, const BlockIO & streams, Context & context)
	{
		const ASTInsertQuery * ast_insert_query = dynamic_cast<const ASTInsertQuery *>(ast.get());

		if (!ast_insert_query)
			throw Exception("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::LOGICAL_ERROR);

		String format = ast_insert_query->format;
		if (format.empty())
			format = "Values";

		/// Data could be in parsed (ast_insert_query.data) and in not parsed yet (istr) part of query.

		buf1 = std::make_unique<ReadBuffer>(
			const_cast<char *>(ast_insert_query->data), ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0, 0);

		if (ast_insert_query->data)
			buffers.push_back(buf1.get());
		buffers.push_back(&istr);

		/** NOTE Must not read from 'istr' before read all between 'ast_insert_query.data' and 'ast_insert_query.end'.
		 * - because 'query.data' could refer to memory piece, used as buffer for 'istr'.
		 */

		data_istr = std::make_unique<ConcatReadBuffer>(buffers);

		res_stream = context.getInputFormat(format, *data_istr, streams.out_sample, context.getSettings().max_insert_block_size);
	}

	Block readImpl() override
	{
		return res_stream->read();
	}

	void readPrefixImpl() override
	{
		return res_stream->readPrefix();
	}

	void readSuffixImpl() override
	{
		return res_stream->readSuffix();
	}

	String getName() const override
	{
		return "InputStreamFromASTInsertQuery";
	}

	String getID() const override
	{
		return "InputStreamFromASTInsertQuery(" + toString(this) + ")";
	}

private:
	ConcatReadBuffer::ReadBuffers buffers;
	std::unique_ptr<ReadBuffer> buf1;
	std::unique_ptr<ReadBuffer> data_istr;

	BlockInputStreamPtr res_stream;
};

}
