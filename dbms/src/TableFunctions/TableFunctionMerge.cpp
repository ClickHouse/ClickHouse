#include <DB/Common/OptimizedRegularExpression.h>

#include <DB/Storages/StorageMerge.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/TableFunctions/ITableFunction.h>
#include <DB/Interpreters/evaluateConstantExpression.h>
#include <DB/Databases/IDatabase.h>
#include <DB/TableFunctions/TableFunctionMerge.h>


namespace DB
{


namespace ErrorCodes
{
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
	extern const int UNKNOWN_TABLE;
}


static NamesAndTypesList chooseColumns(const String & source_database, const String & table_name_regexp_, const Context & context)
{
	OptimizedRegularExpression table_name_regexp(table_name_regexp_);

	StoragePtr any_table;

	{
		auto database = context.getDatabase(source_database);
		auto iterator = database->getIterator();

		while (iterator->isValid())
		{
			if (table_name_regexp.match(iterator->name()))
			{
				any_table = iterator->table();
				break;
			}

			iterator->next();
		}
	}

	if (!any_table)
		throw Exception("Error while executing table function merge. In database " + source_database + " no one matches regular 						 				 expression: " + table_name_regexp_, ErrorCodes::UNKNOWN_TABLE);

	return any_table->getColumnsList();
}


StoragePtr TableFunctionMerge::execute(ASTPtr ast_function, Context & context) const
{
	ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

	if (args_func.size() != 1)
		throw Exception("Storage Merge requires exactly 2 parameters"
			" - name of source database and regexp for table names.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

	if (args.size() != 2)
		throw Exception("Storage Merge requires exactly 2 parameters"
			" - name of source database and regexp for table names.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	args[0] = evaluateConstantExpressionOrIdentidierAsLiteral(args[1], context);
	args[1] = evaluateConstantExpressionAsLiteral(args[1], context);

	String source_database 		= static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
	String table_name_regexp	= static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();

	/// В InterpreterSelectQuery будет создан ExpressionAnalzyer, который при обработке запроса наткнется на этот Identifier.
	/// Нам необходимо его пометить как имя базы данных, поскольку по умолчанию стоит значение column
	typeid_cast<ASTIdentifier &>(*args[0]).kind = ASTIdentifier::Database;

	return StorageMerge::create(
		getName(),
		std::make_shared<NamesAndTypesList>(chooseColumns(source_database, table_name_regexp, context)),
		source_database,
		table_name_regexp,
		context);
}

}
