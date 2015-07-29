#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


namespace DB
{


/** Проверить, существует ли таблица. Вернуть одну строку с одним столбцом result типа UInt8 со значением 0 или 1.
  */
class InterpreterExistsQuery : public IInterpreter
{
public:
	InterpreterExistsQuery(ASTPtr query_ptr_, Context & context_)
		: query_ptr(query_ptr_), context(context_) {}

	BlockIO execute() override
	{
		BlockIO res;
		res.in = executeImpl();
		res.in_sample = getSampleBlock();

		return res;
	}

private:
	ASTPtr query_ptr;
	Context context;

	Block getSampleBlock()
	{
		ColumnWithTypeAndName col;
		col.name = "result";
		col.type = new DataTypeUInt8;
		col.column = col.type->createColumn();

		Block block;
		block.insert(col);

		return block;
	}

	BlockInputStreamPtr executeImpl()
	{
		const ASTExistsQuery & ast = typeid_cast<const ASTExistsQuery &>(*query_ptr);

		bool res = context.isTableExist(ast.database, ast.table);

		ColumnWithTypeAndName col;
		col.name = "result";
		col.type = new DataTypeUInt8;
		col.column = new ColumnConstUInt8(1, res);

		Block block;
		block.insert(col);

		return new OneBlockInputStream(block);
	}
};


}
