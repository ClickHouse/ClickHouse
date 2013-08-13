#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/IO/copyData.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/formatAST.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/bind/placeholders.hpp>


namespace DB
{
InterpreterAlterQuery::InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}

bool namesEqual( const String &name, const ASTPtr & name_type_)
{
	const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*name_type_);
	return name_type.name == name;
}

void InterpreterAlterQuery::execute()
{
	/// Poco::Mutex является рекурсивным, т.е. взятие мьютекса дважды из одного потока не приводит к блокировке
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

	ASTAlterQuery & alter = dynamic_cast<ASTAlterQuery &>(*query_ptr);

	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
	String & table_name = alter.table;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name_escaped = escapeForFileName(table_name);

	StoragePtr table = context.getTable(database_name, table_name);
	String path = context.getPath();
	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";

	ASTPtr attach_ptr = context.getCreateQuery(database_name, table_name);
	ASTCreateQuery & attach = dynamic_cast< ASTCreateQuery &>(*attach_ptr);
	attach.attach = true;
	ASTs & columns = dynamic_cast<ASTExpressionList &>(*attach.columns).children;

	const ASTFunction & storage = dynamic_cast<const ASTFunction &>(*attach.storage);

	String engine_string =  storage.getColumnName();
	Poco::RegularExpression::MatchVec matches;

	const DataTypeFactory & data_type_factory = context.getDataTypeFactory();

	IdentifierNameSet identifier_names;
	attach.storage->collectIdentifierNames(identifier_names);
	for (ASTAlterQuery::ParameterContainer::const_iterator alter_it = alter.parameters.begin();
			alter_it != alter.parameters.end(); ++alter_it)
	{
		const ASTAlterQuery::Parameters & params = *alter_it;

		if (params.type == ASTAlterQuery::ADD)
		{
			const ASTNameTypePair &name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = name_type.type->range;

			/// проверяем корректность типа. В случае некоректного типа будет исключение
			data_type_factory.get(String(type_range.first, type_range.second - type_range.first));

			/// Проверяем, что колонка еще не существует
			if (std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, name_type.name, _1)) != columns.end())
				throw DB::Exception("Wrong column name. Column already exists", DB::ErrorCodes::ILLEGAL_COLUMN);

			/// Проверяем опциональный аргумент AFTER
			if (params.column)
			{
				const ASTIdentifier & col_after = dynamic_cast<const ASTIdentifier &>(*params.column);
				ASTs::iterator insert_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, col_after.name, _1)) ;
				if (insert_it == columns.end())
					throw DB::Exception("Wrong column name. Cannot find column to insert after", DB::ErrorCodes::ILLEGAL_COLUMN);
				else
				{
					/// increase iterator because we want to insert after found element not before
					++insert_it;
					columns.insert(insert_it, params.name_type);
				}
			}
			else
				columns.push_back(params.name_type);
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			const ASTIdentifier & drop_column = dynamic_cast <const ASTIdentifier &>(*params.column);

			/// Проверяем, что поле не является ключевым
			if (identifier_names.find(drop_column.name) != identifier_names.end())
				throw DB::Exception("Cannot drop key column", DB::ErrorCodes::ILLEGAL_COLUMN);

			ASTs::iterator drop_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, drop_column.name, _1));
			if (drop_it == columns.end())
				throw DB::Exception("Wrong column name. Cannot find column to drop", DB::ErrorCodes::ILLEGAL_COLUMN);
			else
				columns.erase(drop_it);
		}

		table->alter(params);

		/// Перезаписываем файл метадата каждую итерацию
		Poco::FileOutputStream ostr(metadata_path);
		formatAST(attach, ostr, 0, false);
	}
}

}
