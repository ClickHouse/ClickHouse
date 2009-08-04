#include <DB/Exception.h>
#include <DB/ErrorCodes.h>
#include <DB/Table.h>
#include <DB/ColumnGroup.h>

#include <DB/StorageNoKey.h>


namespace DB
{
	
StorageNoKey::StorageNoKey(const std::string & path_, const std::string & name_)
	: path(path_),
	name(name_),
	data_file_name(path + name + ".dat"),
	data_file(data_file_name)
{
	/// создаём файлы, если их ещё нет
	data_file.createFile();
}


void StorageNoKey::merge(const AggregatedRowSet & data, const ColumnMask & mask)
{
	if (!table || !column_group)
		throw Exception("Storage was not attached to table and column group",
			ErrorCodes::STORAGE_WAS_NOT_ATTACHED);

	/// просто дописываем данные в конец файла
	Poco::FileOutputStream ostr(data_file_name, std::ios::out | std::ios::binary | std::ios::app);

	for (AggregatedRowSet::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		size_t column_num = 0;
		for (size_t j = 0; j != it->first.size(); ++j)
		{
			if (mask[j])
			{
				table->columns->at(column_group->column_numbers[column_num]).type->serializeBinary(it->first[j], ostr);
				++column_num;
			}
		}

		for (size_t j = 0; j != it->second.size(); ++j)
		{
			if (mask[j + it->first.size()])
			{
				table->columns->at(column_group->column_numbers[column_num]).type->serializeBinary(it->second[j], ostr);
				++column_num;
			}
		}
	}
}


Poco::SharedPtr<ITablePartReader> StorageNoKey::read(const Row & key)
{
	return new StorageNoKeyTablePartReader(key, this);
}


StorageNoKeyTablePartReader::StorageNoKeyTablePartReader(
	const Row & key_, StorageNoKey * pk_)
	: key(key_), pk(pk_), istr(pk->data_file_name)
{
}


bool StorageNoKeyTablePartReader::fetch(Row & row)
{
	if (key.size() > pk->column_group->column_numbers.size())
		throw Exception("Too many columns specified for key", ErrorCodes::TOO_MANY_COLUMNS_FOR_KEY);

	row.resize(pk->column_group->column_numbers.size());

	while (1)
	{
		for (size_t i = 0; i < pk->column_group->column_numbers.size(); ++i)
			pk->table->columns->at(pk->column_group->column_numbers[i]).type->deserializeBinary(row[i], istr);

		if (istr.eof())
			return false;

		if (istr.fail())
			throw Exception("Cannot read data file " + pk->data_file_name
				, ErrorCodes::CANT_READ_DATA_FILE);

		/// проверим, что ключи совпадают (замечание: столбцы ключа всегда идут первыми)
		for (size_t i = 0; i < key.size(); ++i)
			if (key[i] != row[i])
				continue;

		return true;
	}
}

}

