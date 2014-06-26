#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>


namespace DB
{


/** Вернуть одну строку с одним столбцом statement типа String с текстом запроса, создающего указанную таблицу.
	*/
class InterpreterShowCreateQuery
{
public:
	InterpreterShowCreateQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute()
	{
		BlockIO res;
		res.in = executeImpl();
		res.in_sample = getSampleBlock();

		return res;
	}

	BlockInputStreamPtr executeAndFormat(WriteBuffer & buf)
	{
		Block sample = getSampleBlock();
		ASTPtr format_ast = typeid_cast<ASTShowCreateQuery &>(*query_ptr).format;
		String format_name = format_ast ? typeid_cast<ASTIdentifier &>(*format_ast).name : context.getDefaultFormat();

		BlockInputStreamPtr in = executeImpl();
		BlockOutputStreamPtr out = context.getFormatFactory().getOutput(format_name, buf, sample);

		copyData(*in, *out);

		return in;
	}

private:
	ASTPtr query_ptr;
	Context context;

	Block getSampleBlock()
	{
		ColumnWithNameAndType col;
		col.name = "statement";
		col.type = new DataTypeString;
		col.column = col.type->createColumn();

		Block block;
		block.insert(col);

		return block;
	}

	BlockInputStreamPtr executeImpl()
	{
		const ASTShowCreateQuery & ast = typeid_cast<const ASTShowCreateQuery &>(*query_ptr);

		std::stringstream stream;
		formatAST(*context.getCreateQuery(ast.database, ast.table), stream, 0, false, true);
		String res = stream.str();

		ColumnWithNameAndType col;
		col.name = "statement";
		col.type = new DataTypeString;
		col.column = new ColumnConstString(1, res);

		Block block;
		block.insert(col);

		return new OneBlockInputStream(block);
	}
};


}
