#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <DB/Storages/ITableDeclaration.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Interpreters/Context.h>
#include <boost/bind.hpp>

namespace DB
{

bool ITableDeclaration::hasRealColumn(const String &column_name) const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	for (auto & it : real_columns)
		if (it.first == column_name)
			return true;
	return false;
}


Names ITableDeclaration::getColumnNamesList() const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	Names res;
	for (auto & it : real_columns)
		res.push_back(it.first);
	return res;
}


NameAndTypePair ITableDeclaration::getRealColumn(const String &column_name) const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	for (auto & it : real_columns)
		if (it.first == column_name)
			return it;
	throw Exception("There is no column " + column_name + " in table " + getTableName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


bool ITableDeclaration::hasColumn(const String &column_name) const
{
	return hasRealColumn(column_name); /// По умолчанию считаем, что виртуальных столбцов в сторадже нет.
}


NameAndTypePair ITableDeclaration::getColumn(const String &column_name) const
{
	return getRealColumn(column_name); /// По умолчанию считаем, что виртуальных столбцов в сторадже нет.
}


const DataTypePtr ITableDeclaration::getDataTypeByName(const String & column_name) const
{
	const NamesAndTypesList & names_and_types = getColumnsList();
	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
		if (it->first == column_name)
			return it->second;

	throw Exception("There is no column " + column_name + " in table " + getTableName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


Block ITableDeclaration::getSampleBlock() const
{
	Block res;
	const NamesAndTypesList & names_and_types = getColumnsList();

	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
	{
		ColumnWithNameAndType col;
		col.name = it->first;
		col.type = it->second;
		col.column = col.type->createColumn();
		res.insert(col);
	}

	return res;
}


static std::string listOfColumns(const NamesAndTypesList & available_columns)
{
	std::stringstream s;
	for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
	{
		if (it != available_columns.begin())
			s << ", ";
		s << it->first;
	}
	return s.str();
}


typedef google::dense_hash_map<StringRef, const IDataType *, DefaultHash<StringRef> > NamesAndTypesMap;


static NamesAndTypesMap getColumnsMap(const NamesAndTypesList & available_columns)
{
	NamesAndTypesMap res;
	res.set_empty_key(StringRef());

	for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
		res.insert(NamesAndTypesMap::value_type(it->first, &*it->second));

	return res;
}


void ITableDeclaration::check(const Names & column_names) const
{
	const NamesAndTypesList & available_columns = getColumnsList();

	if (column_names.empty())
		throw Exception("Empty list of columns queried for table " + getTableName()
			+ ". There are columns: " + listOfColumns(available_columns),
			ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);

	typedef google::dense_hash_set<StringRef, DefaultHash<StringRef> > UniqueStrings;
	UniqueStrings unique_names;
	unique_names.set_empty_key(StringRef());

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		if (columns_map.end() == columns_map.find(*it))
			throw Exception("There is no column with name " + *it + " in table " + getTableName()
			+ ". There are columns: " + listOfColumns(available_columns),
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (unique_names.end() != unique_names.find(*it))
			throw Exception("Column " + *it + " queried more than once in table " + getTableName(),
				ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
		unique_names.insert(*it);
	}
}


void ITableDeclaration::check(const Block & block, bool need_all) const
{
	const NamesAndTypesList & available_columns = getColumnsList();
	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);

	typedef std::unordered_set<String> NameSet;
	NameSet names_in_block;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);

		if (names_in_block.count(column.name))
			throw Exception("Duplicate column  " + column.name + " in block",
							ErrorCodes::DUPLICATE_COLUMN);

		names_in_block.insert(column.name);

		NamesAndTypesMap::const_iterator it = columns_map.find(column.name);
		if (columns_map.end() == it)
			throw Exception("There is no column with name " + column.name + " in table " + getTableName()
				+ ". There are columns: " + listOfColumns(available_columns),
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (column.type->getName() != it->second->getName())
			throw Exception("Type mismatch for column " + column.name + " in table " + getTableName()
				+ ". Column has type " + it->second->getName() + ", got type " + column.type->getName(),
				ErrorCodes::TYPE_MISMATCH);
	}

	if (need_all && names_in_block.size() < columns_map.size())
	{
		for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
		{
			if (!names_in_block.count(it->first))
				throw Exception("Expected column " + it->first, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
		}
	}
}

/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
{
	String name_with_dot = name_without_dot + ".";
	return (name_with_dot == name_type.first.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.first);
}

void ITableDeclaration::alterColumns(const ASTAlterQuery::Parameters & params, NamesAndTypesListPtr & columns, const Context & context)
{
	if (params.type == ASTAlterQuery::ADD)
	{
		NamesAndTypesList::iterator insert_it = columns->end();
		if (params.column)
		{
			String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;

			/// Пытаемся найти первую с конца колонку с именем column_name или с именем, начинающимся с column_name и ".".
			/// Например "fruits.bananas"
			NamesAndTypesList::reverse_iterator reverse_insert_it = std::find_if(columns->rbegin(), columns->rend(),  boost::bind(namesEqual, column_name, _1) );

			if (reverse_insert_it == columns->rend())
				throw Exception("Wrong column name. Cannot find column " + column_name + " to insert after", DB::ErrorCodes::ILLEGAL_COLUMN);
			else
			{
				/// base возвращает итератор уже смещенный на один элемент вправо
				insert_it = reverse_insert_it.base();
			}
		}

		const ASTNameTypePair & ast_name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
		StringRange type_range = ast_name_type.type->range;
		String type_string = String(type_range.first, type_range.second - type_range.first);

		DB::DataTypePtr data_type = context.getDataTypeFactory().get(type_string);
		NameAndTypePair pair(ast_name_type.name, data_type );
		columns->insert(insert_it, pair);

		/// Медленно, так как каждый раз копируется список
		columns = DataTypeNested::expandNestedColumns(*columns);
		return;
	}
	else if (params.type == ASTAlterQuery::DROP)
	{
		String column_name = dynamic_cast<const ASTIdentifier &>(*(params.column)).name;

		/// Удаляем колонки из листа columns
		bool is_first = true;
		NamesAndTypesList::iterator column_it;
		do
		{
			column_it = std::find_if(columns->begin(), columns->end(), boost::bind(namesEqual, column_name, _1));

			if (column_it == columns->end())
			{
				if (is_first)
					throw Exception("Wrong column name. Cannot find column " + column_name + " to drop", DB::ErrorCodes::ILLEGAL_COLUMN);
			}
			else
				columns->erase(column_it);
			is_first = false;
		}
		while (column_it != columns->end());
	}
	else if (params.type == ASTAlterQuery::MODIFY)
	{
		const ASTNameTypePair & ast_name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
		StringRange type_range = ast_name_type.type->range;
		String type_string = String(type_range.first, type_range.second - type_range.first);

		DB::DataTypePtr data_type = context.getDataTypeFactory().get(type_string);
		NameAndTypePair pair(ast_name_type.name, data_type);
		NamesAndTypesList::iterator column_it = std::find_if(columns->begin(), columns->end(), boost::bind(namesEqual, ast_name_type.name, _1) );
		if (column_it == columns->end())
			throw Exception("Wrong column name. Cannot find column " + ast_name_type.name + " to modify.",  DB::ErrorCodes::ILLEGAL_COLUMN);
		column_it->second = data_type;
	}
	else
		throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}

}
