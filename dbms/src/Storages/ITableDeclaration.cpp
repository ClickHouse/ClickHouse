#include <unordered_set>
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

bool ITableDeclaration::hasRealColumn(const String & column_name) const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	for (auto & it : real_columns)
		if (it.name == column_name)
			return true;
	return false;
}


Names ITableDeclaration::getColumnNamesList() const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	Names res;
	for (auto & it : real_columns)
		res.push_back(it.name);
	return res;
}


NameAndTypePair ITableDeclaration::getRealColumn(const String & column_name) const
{
	const NamesAndTypesList & real_columns = getColumnsList();
	for (auto & it : real_columns)
		if (it.name == column_name)
			return it;
	throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


bool ITableDeclaration::hasColumn(const String & column_name) const
{
	return hasRealColumn(column_name); /// По умолчанию считаем, что виртуальных столбцов в сторадже нет.
}


NameAndTypePair ITableDeclaration::getColumn(const String & column_name) const
{
	return getRealColumn(column_name); /// По умолчанию считаем, что виртуальных столбцов в сторадже нет.
}


const DataTypePtr ITableDeclaration::getDataTypeByName(const String & column_name) const
{
	const NamesAndTypesList & names_and_types = getColumnsList();
	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
		if (it->name == column_name)
			return it->type;

	throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


Block ITableDeclaration::getSampleBlock() const
{
	Block res;
	const NamesAndTypesList & names_and_types = getColumnsList();

	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
	{
		ColumnWithNameAndType col;
		col.name = it->name;
		col.type = it->type;
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
		s << it->name;
	}
	return s.str();
}


typedef google::dense_hash_map<StringRef, const IDataType *, StringRefHash> NamesAndTypesMap;


static NamesAndTypesMap getColumnsMap(const NamesAndTypesList & available_columns)
{
	NamesAndTypesMap res;
	res.set_empty_key(StringRef());

	for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
		res.insert(NamesAndTypesMap::value_type(it->name, &*it->type));

	return res;
}


void ITableDeclaration::check(const Names & column_names) const
{
	const NamesAndTypesList & available_columns = getColumnsList();

	if (column_names.empty())
		throw Exception("Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
			ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);

	typedef google::dense_hash_set<StringRef, StringRefHash> UniqueStrings;
	UniqueStrings unique_names;
	unique_names.set_empty_key(StringRef());

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		if (columns_map.end() == columns_map.find(*it))
			throw Exception("There is no column with name " + *it + " in table. There are columns: " + listOfColumns(available_columns),
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (unique_names.end() != unique_names.find(*it))
			throw Exception("Column " + *it + " queried more than once",
				ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
		unique_names.insert(*it);
	}
}


void ITableDeclaration::check(const NamesAndTypesList & columns) const
{
	const NamesAndTypesList & available_columns = getColumnsList();
	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);

	typedef google::dense_hash_set<StringRef, StringRefHash> UniqueStrings;
	UniqueStrings unique_names;
	unique_names.set_empty_key(StringRef());

	for (const NameAndTypePair & column : columns)
	{
		NamesAndTypesMap::const_iterator it = columns_map.find(column.name);
		if (columns_map.end() == it)
			throw Exception("There is no column with name " + column.name + ". There are columns: "
				+ listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (column.type->getName() != it->second->getName())
			throw Exception("Type mismatch for column " + column.name + ". Column has type "
				+ it->second->getName() + ", got type " + column.type->getName(), ErrorCodes::TYPE_MISMATCH);

		if (unique_names.end() != unique_names.find(column.name))
			throw Exception("Column " + column.name + " queried more than once",
				ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
		unique_names.insert(column.name);
	}
}


void ITableDeclaration::check(const NamesAndTypesList & columns, const Names & column_names) const
{
	if (column_names.empty())
		throw Exception("Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
			ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

	const NamesAndTypesList & available_columns = getColumnsList();
	const NamesAndTypesMap & available_columns_map = getColumnsMap(available_columns);
	const NamesAndTypesMap & provided_columns_map = getColumnsMap(columns);

	typedef google::dense_hash_set<StringRef, StringRefHash> UniqueStrings;
	UniqueStrings unique_names;
	unique_names.set_empty_key(StringRef());

	for (const String & name : column_names)
	{
		NamesAndTypesMap::const_iterator it = provided_columns_map.find(name);
		if (provided_columns_map.end() == it)
			continue;

		NamesAndTypesMap::const_iterator jt = available_columns_map.find(name);
		if (available_columns_map.end() == jt)
			throw Exception("There is no column with name " + name + ". There are columns: "
				+ listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (it->second->getName() != jt->second->getName())
			throw Exception("Type mismatch for column " + name + ". Column has type "
				+ jt->second->getName() + ", got type " + it->second->getName(), ErrorCodes::TYPE_MISMATCH);

		if (unique_names.end() != unique_names.find(name))
			throw Exception("Column " + name + " queried more than once",
				ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
		unique_names.insert(name);
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
			throw Exception("There is no column with name " + column.name + ". There are columns: "
				+ listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		if (column.type->getName() != it->second->getName())
			throw Exception("Type mismatch for column " + column.name + ". Column has type "
				+ it->second->getName() + ", got type " + column.type->getName(), ErrorCodes::TYPE_MISMATCH);
	}

	if (need_all && names_in_block.size() < columns_map.size())
	{
		for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
		{
			if (!names_in_block.count(it->name))
				throw Exception("Expected column " + it->name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
		}
	}
}

}
