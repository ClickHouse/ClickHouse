#include <DB/Interpreters/InterpreterCheckQuery.h>
#include <DB/Parsers/ASTCheckQuery.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

InterpreterCheckQuery::InterpreterCheckQuery(DB::ASTPtr query_ptr_, DB::Context& context_) : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterCheckQuery::execute()
{
	ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

	StoragePtr table = context.getTable(database_name, table_name);

	result = Block{{ new ColumnUInt8, new DataTypeUInt8, "result" }};
	result.getByPosition(0).column->insert(Field(UInt64(table->checkData())));

	BlockIO res;
	res.in = new OneBlockInputStream(result);
	res.in_sample = result.cloneEmpty();

	return res;
}

}
