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
#include <DB/Storages/StorageMerge.h>
#include <DB/Storages/StorageMergeTree.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/bind/placeholders.hpp>


using namespace DB;

InterpreterAlterQuery::InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}

static bool namesEqual(const String &name, const ASTPtr & name_type_)
{
	const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*name_type_);
	return name_type.name == name;
}

/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
static bool namesEqualIgnoreAfterDot(const String & name_without_dot, const ASTPtr & name_type_)
{
	const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*name_type_);

	String name_with_dot = name_without_dot + ".";
	return (name_without_dot == name_type.name || name_with_dot == name_type.name.substr(0, name_with_dot.length()));
}

void InterpreterAlterQuery::dropColumnFromAST(const ASTIdentifier & drop_column, ASTs & columns)
{
	Exception e("Wrong column name. Cannot find column " +  drop_column.name + " to drop", DB::ErrorCodes::ILLEGAL_COLUMN);
	ASTs::iterator drop_it;

	size_t dot_pos = drop_column.name.find('.');
	/// случай удаления nested столбца
	if (dot_pos != std::string::npos)
	{
		/// в Distributed таблицах столбцы имеют название "nested.column"
		drop_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, drop_column.name, _1));
		if (drop_it != columns.end())
			columns.erase(drop_it);
		else
		{
			try
			{
				/// в MergeTree таблицах есть ASTFunction "nested"
				/// в аргументах которой записаны столбцы
				ASTs::iterator nested_it;
				std::string nested_base_name = drop_column.name.substr(0, dot_pos);
				nested_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, nested_base_name, _1));
				if (nested_it == columns.end())
					throw e;

				if ((**nested_it).children.size() != 1)
					throw e;

				ASTFunction & f = dynamic_cast<ASTFunction &>(*(**nested_it).children.back());
				if (f.name != "Nested")
					throw e;

				ASTs & nested_columns = dynamic_cast<ASTExpressionList &>(*f.arguments).children;

				drop_it = std::find_if(nested_columns.begin(), nested_columns.end(), boost::bind(namesEqual, drop_column.name.substr(dot_pos + 1), _1));
				if (drop_it == nested_columns.end())
					throw e;
				else
					nested_columns.erase(drop_it);

				if (nested_columns.empty())
					columns.erase(nested_it);
			}
			catch (std::bad_cast & bad_cast_err)
			{
				throw e;
			}
		}
	}
	else
	{
		drop_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, drop_column.name, _1));
		if (drop_it == columns.end())
			throw e;
		else
			columns.erase(drop_it);
	}
}

void addColumnToAST1(ASTs & columns, const ASTPtr & add_column_ptr, const ASTPtr & after_column_ptr)
{
	const ASTNameTypePair & add_column = dynamic_cast<const ASTNameTypePair &>(*add_column_ptr);
	const ASTIdentifier * col_after = after_column_ptr ? &dynamic_cast<const ASTIdentifier &>(*after_column_ptr) : nullptr;

	if (std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, add_column.name, _1)) != columns.end())
	{
		throw Exception("Fail to add column " + add_column.name + ". Column already exists");
	}
	ASTs::iterator insert_it = columns.end();
	if (col_after)
	{
		/// если есть точка, то нас просят вставить после nested столбца
		auto find_functor = col_after->name.find('.') != std::string ::npos ? boost::bind(namesEqual, col_after->name, _1) : boost::bind(namesEqualIgnoreAfterDot, col_after->name, _1);

		insert_it = std::find_if(columns.begin(), columns.end(), find_functor);
		if (insert_it == columns.end())
			throw Exception("Wrong column name. Cannot find column " + col_after->name + " to insert after");
	}
	columns.insert(insert_it, add_column_ptr);
}

void InterpreterAlterQuery::addColumnToAST(StoragePtr table, ASTs & columns, const ASTPtr & add_column_ptr, const ASTPtr & after_column_ptr)
{
	/// хотим исключение если приведение зафейлится
	const ASTNameTypePair & add_column = dynamic_cast<const ASTNameTypePair &>(*add_column_ptr);
	const ASTIdentifier * after_col = after_column_ptr ? &dynamic_cast<const ASTIdentifier &>(*after_column_ptr) : nullptr;

	size_t dot_pos = add_column.name.find('.');
	bool insert_nested_column = dot_pos != std::string::npos;

	if (insert_nested_column)
	{
		const DataTypeFactory & data_type_factory = context.getDataTypeFactory();
		StringRange type_range = add_column.type->range;
		String type(type_range.first, type_range.second - type_range.first);
		if (!dynamic_cast<DataTypeArray *>(data_type_factory.get(type).get()))
		{
			throw Exception("Cannot add column " + add_column.name + ". Because it is not an array. Only arrays could be nested and consist '.' in their names");
		}
	}

	if (dynamic_cast<StorageMergeTree *>(table.get()) && insert_nested_column)
	{
		/// специальный случай для вставки nested столбцов в MergeTree
		/// в MergeTree таблицах есть ASTFunction "Nested" в аргументах которой записаны столбцы
		std::string nested_base_name = add_column.name.substr(0, dot_pos);
		ASTs::iterator nested_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, nested_base_name, _1));

		if (nested_it != columns.end())
		{
			/// нужно добавить колонку в уже существующий nested столбец
			ASTFunction * nested_func = dynamic_cast<ASTFunction *>((*nested_it)->children.back().get());
			if (!(**nested_it).children.size() || !nested_func || nested_func->name != "Nested")
				throw Exception("Column with name " + nested_base_name + " already exists. But it is not nested.");

			ASTs & nested_columns = dynamic_cast<ASTExpressionList &>(*nested_func->arguments).children;

			ASTPtr new_nested_column = add_column_ptr->clone();
			dynamic_cast<ASTNameTypePair &>(*new_nested_column).name = add_column.name.substr(dot_pos + 1);
			ASTPtr new_after_column = after_column_ptr ? after_column_ptr->clone() : nullptr;

			if (new_after_column)
			{
				size_t after_dot_pos = after_col->name.find('.');
				if (after_dot_pos == std::string::npos)
					throw Exception("Nested column " + add_column.name + " should be inserted only after nested column");
				if (add_column.name.substr(0, dot_pos) != after_col->name.substr(0, after_dot_pos))
					throw Exception("Nested column " + add_column.name + "should be inserted after column with the same name before the '.'");

				dynamic_cast<ASTIdentifier &>(*new_after_column).name = after_col->name.substr(after_dot_pos + 1);
			}
			addColumnToAST1(nested_columns, new_nested_column, new_after_column);
		}
		else
		{
			throw Exception("If you want to create new Nested column use syntax like. ALTER TABLE table ADD COLUMN MyColumn Nested(Name1 Type1, Name2 Type2...) [AFTER BeforeColumn]");
		}
	}
	else
	{
		/// в Distributed и Merge таблицах столбцы имеют название "nested.column"
		addColumnToAST1(columns, add_column_ptr, after_column_ptr);
	}
}

void InterpreterAlterQuery::execute()
{
	ASTAlterQuery & alter = dynamic_cast<ASTAlterQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

	StoragePtr table = context.getTable(database_name, table_name);
	auto table_soft_lock = table->lockDataForAlter();

	const DataTypeFactory & data_type_factory = context.getDataTypeFactory();
	String path = context.getPath();

	String database_name_escaped = escapeForFileName(database_name);
	String table_name_escaped = escapeForFileName(table_name);

	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";

	ASTPtr attach_ptr = context.getCreateQuery(database_name, table_name);
	ASTCreateQuery & attach = dynamic_cast< ASTCreateQuery &>(*attach_ptr);
	attach.attach = true;
	ASTs & columns = dynamic_cast<ASTExpressionList &>(*attach.columns).children;

	/// Различные проверки, на возможность выполнения запроса
	ASTs columns_copy;
	for (const auto & ast : columns)
		columns_copy.push_back(ast->clone());

	IdentifierNameSet identifier_names;
	attach.storage->collectIdentifierNames(identifier_names);
	for (ASTAlterQuery::ParameterContainer::const_iterator alter_it = alter.parameters.begin();
			alter_it != alter.parameters.end(); ++alter_it)
	{
		const ASTAlterQuery::Parameters & params = *alter_it;

		if (params.type == ASTAlterQuery::ADD)
		{
			addColumnToAST(table, columns_copy, params.name_type, params.column);
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			const ASTIdentifier & drop_column = dynamic_cast <const ASTIdentifier &>(*params.column);

			/// Проверяем, что поле не является ключевым
			if (identifier_names.find(drop_column.name) != identifier_names.end())
				throw Exception("Cannot drop key column", DB::ErrorCodes::ILLEGAL_COLUMN);

			dropColumnFromAST(drop_column, columns_copy);
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = name_type.type->range;

			/// проверяем корректность типа. В случае некоректного типа будет исключение
			String type(type_range.first, type_range.second - type_range.first);
			data_type_factory.get(type);

			/// проверяем, что колонка существует
			auto modified_column = std::find_if(columns_copy.begin(), columns_copy.end(), boost::bind(namesEqual, name_type.name, _1));
			if ( modified_column == columns_copy.end())
				throw Exception("Wrong column name. Column " + name_type.name  + " not exists", DB::ErrorCodes::ILLEGAL_COLUMN);

			/// Проверяем, что поле не является ключевым
			if (identifier_names.find(name_type.name) != identifier_names.end())
				throw Exception("Modification of primary column not supported", DB::ErrorCodes::ILLEGAL_COLUMN);

			/// к сожалению, проверить на возможно ли это приведение типов можно только во время выполнения
		}
	}

	/// Пока разрешим читать из таблицы. Запретим при первой попытке изменить структуру таблицы.
	/// Это позволит сделать большую часть первого MODIFY, не останавливая чтение из таблицы.
	IStorage::TableStructureWriteLockPtr table_hard_lock;

	/// todo cycle over sub tables and tables
	/// Применяем изменения
	for (ASTAlterQuery::ParameterContainer::const_iterator alter_it = alter.parameters.begin();
			alter_it != alter.parameters.end(); ++alter_it)
	{
		const ASTAlterQuery::Parameters & params = *alter_it;

		if (params.type == ASTAlterQuery::MODIFY)
		{
			table->prepareAlterModify(params);

			if (!table_hard_lock)
				table_hard_lock = table->lockStructureForAlter();

			table->commitAlterModify(params);
		}
		else
		{
			if (!table_hard_lock)
				table_hard_lock = table->lockStructureForAlter();

			table->alter(params);
		}
		
		if (params.type == ASTAlterQuery::ADD)
		{
			addColumnToAST(table, columns, params.name_type, params.column);
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			const ASTIdentifier & drop_column = dynamic_cast <const ASTIdentifier &>(*params.column);
			dropColumnFromAST(drop_column, columns);
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
			ASTs::iterator modify_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, name_type.name, _1));
			ASTNameTypePair & modified_column = dynamic_cast<ASTNameTypePair &>(**modify_it);
			modified_column.type = name_type.type;
		}

		/// Перезаписываем файл метадата каждую итерацию
		Poco::FileOutputStream ostr(metadata_path);
		formatAST(attach, ostr, 0, false);
	}
}
