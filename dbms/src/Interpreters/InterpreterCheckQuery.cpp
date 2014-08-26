#include <DB/Interpreters/InterpreterCheckQuery.h>
#include <DB/Parsers/ASTCheckQuery.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

using namespace DB;

InterpreterCheckQuery::InterpreterCheckQuery(DB::ASTPtr query_ptr_, DB::Context& context_) : query_ptr(query_ptr_), context(context_)
{
}

BlockInputStreamPtr InterpreterCheckQuery::execute()
{
	/// @TODO
	ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

	StoragePtr table = context.getTable(database_name, table_name);

	result = getSampleBlock();
	result.getByPosition(0).column->insert(Field(UInt64(table->checkData())));

	return BlockInputStreamPtr(new OneBlockInputStream(result));
}

Block InterpreterCheckQuery::getSampleBlock()
{
	DB::Block b;
	ColumnPtr column(new ColumnUInt8);
	b.insert(ColumnWithNameAndType(column, new DataTypeUInt8, "result"));
	return b;
}
