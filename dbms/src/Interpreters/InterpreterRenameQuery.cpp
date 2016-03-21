#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>

#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Databases/IDatabase.h>

#include <DB/Interpreters/InterpreterRenameQuery.h>



namespace DB
{


InterpreterRenameQuery::InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


struct RenameDescription
{
	RenameDescription(const ASTRenameQuery::Element & elem, const String & path, const String & current_database) :
		from_database_name(elem.from.database.empty() ? current_database : elem.from.database),
		from_table_name(elem.from.table),
		to_database_name(elem.to.database.empty() ? current_database : elem.to.database),
		to_table_name(elem.to.table)
	{}

	String from_database_name;
	String from_table_name;

	String to_database_name;
	String to_table_name;
};


BlockIO InterpreterRenameQuery::execute()
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	ASTRenameQuery & rename = typeid_cast<ASTRenameQuery &>(*query_ptr);

	/** Если в процессе переименования произошла ошибка, то может быть переименована только часть таблиц,
	  *  или состояние может стать неконсистентным. (Это имеет смысл исправить.)
	  */

	std::vector<RenameDescription> descriptions;
	descriptions.reserve(rename.elements.size());

	/// Для того, чтобы захватывать блокировки таблиц в одном и том же порядке в разных RENAME-ах.
	struct UniqueTableName
	{
		String database_name;
		String table_name;

		UniqueTableName(const String & database_name_, const String & table_name_)
			: database_name(database_name_), table_name(table_name_) {}

		bool operator< (const UniqueTableName & rhs) const
		{
			return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
		}
	};

	std::set<UniqueTableName> unique_tables;

	for (const auto & elem : rename.elements)
	{
		descriptions.emplace_back(elem, path, current_database);
		unique_tables.emplace(descriptions.back().from_database_name, descriptions.back().from_table_name);
	}

	std::vector<IStorage::TableFullWriteLockPtr> locks;
	locks.reserve(unique_tables.size());

	for (const auto & names : unique_tables)
		if (auto table = context.tryGetTable(names.database_name, names.table_name))
			locks.emplace_back(table->lockForAlter());

	/** Все таблицы заблокированы. Теперь можно блокировать Context. Порядок важен, чтобы избежать deadlock-ов.
	  * Это обеспечивает атомарность всех указанных RENAME с точки зрения пользователя СУБД,
	  *  но лишь в случаях, когда в процессе переименования не было исключений и сервер не падал.
	  */

	auto lock = context.getLock();

	for (const auto & elem : descriptions)
	{
		context.assertTableDoesntExist(elem.to_database_name, elem.to_table_name);

		context.getDatabase(elem.from_database_name)->renameTable(
			context, elem.from_table_name, *context.getDatabase(elem.to_database_name), elem.to_table_name);
	}

	return {};
}


}
