#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ProjectionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Interpreters/Expression.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>


namespace DB
{


InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_)
	: query_ptr(query_ptr_), context(context_), max_block_size(max_block_size_)
{
}


StoragePtr InterpreterSelectQuery::getTable()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);
	
	/// Из какой таблицы читать данные. JOIN-ы не поддерживаются.

	String database_name;
	String table_name;

	/** Если таблица не указана - используем таблицу system.one.
	  * Если база данных не указана - используем текущую базу данных.
	  */
	if (!query.table)
	{
		database_name = "system";
		table_name = "one";
	}
	else if (!query.database)
		database_name = context.current_database;

	if (query.database)
		database_name = dynamic_cast<ASTIdentifier &>(*query.database).name;
	if (query.table)
		table_name = dynamic_cast<ASTIdentifier &>(*query.table).name;

	if (context.databases->end() == context.databases->find(database_name)
		|| (*context.databases)[database_name].end() == (*context.databases)[database_name].find(table_name))
		throw Exception("Unknown table " + table_name + " in database " + database_name, ErrorCodes::UNKNOWN_TABLE);

	return (*context.databases)[database_name][table_name];
}


DataTypes InterpreterSelectQuery::getReturnTypes()
{
	context.columns = getTable()->getColumns();
	Expression expression(dynamic_cast<ASTSelectQuery &>(*query_ptr).select_expression_list, context);
	return expression.getReturnTypes();
}


BlockInputStreamPtr InterpreterSelectQuery::execute()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);

	StoragePtr table = getTable();

	/// Какие столбцы читать из этой таблицы

	context.columns = table->getColumns();
	Poco::SharedPtr<Expression> expression = new Expression(query_ptr, context);
	BlockInputStreamPtr stream = table->read(expression->getRequiredColumns(), query_ptr, max_block_size);

	/// Если есть условие WHERE - сначала выполним часть выражения, необходимую для его вычисления
	if (query.where_expression)
	{
		setPartID(query.where_expression, PART_WHERE);
		stream = new ExpressionBlockInputStream(stream, expression, PART_WHERE);
		stream = new FilterBlockInputStream(stream);
	}

	/// Выполним оставшуюся часть выражения
	stream = new ExpressionBlockInputStream(stream, expression);

	setPartID(query.select_expression_list, PART_SELECT);
	stream = new ProjectionBlockInputStream(stream, expression, PART_SELECT);

	return stream;
}


void InterpreterSelectQuery::setPartID(ASTPtr ast, unsigned part_id)
{
	ast->part_id = part_id;
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		setPartID(*it, part_id);
}

}
