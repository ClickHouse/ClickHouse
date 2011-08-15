#include <set>

#include <DB/Storages/IStorage.h>


namespace DB
{

static std::string listOfColumns(const NamesAndTypes & available_columns)
{
	std::stringstream s;
	for (NamesAndTypes::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
	{
		if (it != available_columns.begin())
			s << ", ";
		s << it->first;
	}
	return s.str();
}


void IStorage::check(const Names & column_names) const
{
	const NamesAndTypes & available_columns = getColumns();
	
	if (column_names.empty())
		throw Exception("Empty list of columns queried for table " + getTableName()
			+ ". There are columns: " + listOfColumns(available_columns),
			ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

	typedef std::set<std::string> UniqueStrings;
	UniqueStrings unique_names;

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		if (available_columns.end() == available_columns.find(*it))
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
	const NamesAndTypes & available_columns = getColumns();
	
	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);

		NamesAndTypes::const_iterator it = available_columns.find(column.name);
		if (available_columns.end() == it)
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
