#include <DB/Storages/IStorage.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/InterpreterExistsQuery.h>


namespace DB
{

BlockIO InterpreterExistsQuery::execute()
{
	BlockIO res;
	res.in = executeImpl();
	res.in_sample = getSampleBlock();

	return res;
}


Block InterpreterExistsQuery::getSampleBlock()
{
	return {{ std::make_shared<ColumnConstUInt8>(0, 0), std::make_shared<DataTypeUInt8>(), "result" }};
}


BlockInputStreamPtr InterpreterExistsQuery::executeImpl()
{
	const ASTExistsQuery & ast = typeid_cast<const ASTExistsQuery &>(*query_ptr);
	bool res = context.isTableExist(ast.database, ast.table);

	return std::make_shared<OneBlockInputStream>(Block{{
		std::make_shared<ColumnConstUInt8>(1, res),
		std::make_shared<DataTypeUInt8>(),
		"result" }});
}

}
