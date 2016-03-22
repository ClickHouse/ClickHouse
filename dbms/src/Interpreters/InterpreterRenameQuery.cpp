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

	std::set<UniqueTableName> unique_tables_from;

	/// Не даёт удалять переименовываемые таблицы или создавать таблицы на месте, куда они переименовываются.
	std::map<UniqueTableName, std::unique_ptr<DDLGuard>> table_guards;

	for (const auto & elem : rename.elements)
	{
		descriptions.emplace_back(elem, path, current_database);

		UniqueTableName from(descriptions.back().from_database_name, descriptions.back().from_table_name);
		UniqueTableName to(descriptions.back().to_database_name, descriptions.back().to_table_name);

		unique_tables_from.emplace(from);

		if (!table_guards.count(from))
			table_guards.emplace(from,
				context.getDDLGuard(
					from.database_name,
					from.table_name,
					"Table " + from.database_name + "." + from.table_name + " is being renamed right now"));

		if (!table_guards.count(to))
			table_guards.emplace(to,
				context.getDDLGuard(
					to.database_name,
					to.table_name,
					"Some table right now is being renamed to " + to.database_name + "." + to.table_name));
	}

	std::vector<IStorage::TableFullWriteLockPtr> locks;
	locks.reserve(unique_tables_from.size());

	for (const auto & names : unique_tables_from)
		if (auto table = context.tryGetTable(names.database_name, names.table_name))
			locks.emplace_back(table->lockForAlter());

	/** Все таблицы заблокированы. Если переименований больше одного в цепочке, то
	  *  на время их проведения, надо взять глобальную блокировку. Порядок важен, чтобы избежать deadlock-ов.
	  * Это обеспечивает атомарность всех указанных RENAME с точки зрения пользователя СУБД,
	  *  но лишь в случаях, когда в процессе переименования не было исключений и сервер не падал.
	  */

	decltype(context.getLock()) lock;

	if (descriptions.size() > 1)
		lock = context.getLock();

	for (const auto & elem : descriptions)
	{
		context.assertTableDoesntExist(elem.to_database_name, elem.to_table_name);

		context.getDatabase(elem.from_database_name)->renameTable(
			context, elem.from_table_name, *context.getDatabase(elem.to_database_name), elem.to_table_name);
	}

	return {};
}


}
