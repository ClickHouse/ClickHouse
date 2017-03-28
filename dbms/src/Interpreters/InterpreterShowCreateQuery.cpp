#include <DB/Storages/IStorage.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/formatAST.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Interpreters/InterpreterShowCreateQuery.h>


namespace DB
{

BlockIO InterpreterShowCreateQuery::execute()
{
	BlockIO res;
	res.in = executeImpl();
	res.in_sample = getSampleBlock();

	return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
	return {{ std::make_shared<ColumnConstString>(0, String()), std::make_shared<DataTypeString>(), "statement" }};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
	const ASTShowCreateQuery & ast = typeid_cast<const ASTShowCreateQuery &>(*query_ptr);

	std::stringstream stream;
	formatAST(*context.getCreateQuery(ast.database, ast.table), stream, 0, false, true);
	String res = stream.str();

	return std::make_shared<OneBlockInputStream>(Block{{
		std::make_shared<ColumnConstString>(1, res),
		std::make_shared<DataTypeString>(),
		"statement"}});
}

}
