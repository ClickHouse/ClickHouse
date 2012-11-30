#include <set>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

#include <DB/Storages/IStorage.h>


namespace DB
{


const DataTypePtr IStorage::getDataTypeByName(const String & column_name) const
{
	const NamesAndTypesList & names_and_types = getColumnsList();
	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
		if (it->first == column_name)
			return it->second;
		
	throw Exception("There is no column " + column_name + " in table " + getTableName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


Block IStorage::getSampleBlock() const
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


typedef google::dense_hash_map<StringRef, const IDataType *, StringRefHash> NamesAndTypesMap;


static NamesAndTypesMap getColumnsMap(const NamesAndTypesList & available_columns)
{
	NamesAndTypesMap res;

	for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
		res.insert(NamesAndTypesMap::value_type(it->first, &*it->second));

	return res;
}


void IStorage::check(const Names & column_names) const
{
	const NamesAndTypesList & available_columns = getColumnsList();
	
	if (column_names.empty())
		throw Exception("Empty list of columns queried for table " + getTableName()
			+ ". There are columns: " + listOfColumns(available_columns),
			ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);

	typedef google::dense_hash_set<StringRef, StringRefHash> UniqueStrings;
	UniqueStrings unique_names;

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


void IStorage::check(const Block & block) const
{
	const NamesAndTypesList & available_columns = getColumnsList();
	const NamesAndTypesMap & columns_map = getColumnsMap(available_columns);
	
	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);

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
}


}
