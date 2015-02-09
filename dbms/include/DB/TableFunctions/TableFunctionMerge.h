#pragma once

#include <statdaemons/OptimizedRegularExpression.h>

#include <DB/Storages/StorageFactory.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/TableFunctions/ITableFunction.h>
#include <DB/Interpreters/reinterpretAsIdentifier.h>


namespace DB
{

/*
 * merge(db_name, tables_regexp)- создаёт временный StorageMerge.
 * Cтруктура таблицы берётся из первой попавшейся таблицы, подходящей под регексп.
 * Если такой таблицы нет - кидается исключение.
 */

class TableFunctionMerge: public ITableFunction
{
public:
	std::string getName() const override { return "merge"; }

	StoragePtr execute(ASTPtr ast_function, Context & context) const override
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

		String source_database 		= reinterpretAsIdentifier(args[0], context).name;
		String table_name_regexp	= safeGet<const String &>(typeid_cast<ASTLiteral &>(*args[1]).value);

		/// В InterpreterSelectQuery будет создан ExpressionAnalzyer, который при обработке запроса наткнется на этот Identifier.
		/// Нам необходимо его пометить как имя базы данных, поскольку по умолчанию стоит значение column
		typeid_cast<ASTIdentifier &>(*args[0]).kind = ASTIdentifier::Database;

		return StorageMerge::create(getName(), chooseColumns(source_database, table_name_regexp, context),
			source_database, table_name_regexp, context);
	}

private:
	NamesAndTypesListPtr chooseColumns(const String & source_database, const String & table_name_regexp_, Context & context) const
	{
		OptimizedRegularExpression table_name_regexp(table_name_regexp_);

		StoragePtr any_table;

		{
			/// Список таблиц могут менять в другом потоке.
			Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
			context.assertDatabaseExists(source_database);
			const Tables & tables = context.getDatabases().at(source_database);
			for (Tables::const_iterator it = tables.begin(); it != tables.end(); ++it)
				if (table_name_regexp.match(it->first))
				{
					any_table = it->second;
					break;
				}
		}

		if (!any_table)
			throw Exception("Error while executing table function merge. In database " + source_database + " no one matches regular 						 				 expression: " + table_name_regexp_, ErrorCodes::UNKNOWN_TABLE);

		auto table_lock = any_table->lockStructure(false);
		return new NamesAndTypesList(any_table->getColumnsList());
	}
};


}
