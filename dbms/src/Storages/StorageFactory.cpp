#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageMemory.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>


namespace DB
{


StoragePtr StorageFactory::get(
	const String & name,
	const String & data_path,
	const String & table_name,
	NamesAndTypesListPtr columns) const
{
	if (name == "Log")
		return new StorageLog(data_path, table_name, columns);
	else if (name == "Memory")
		return new StorageMemory(table_name, columns);
	else if (name == "SystemNumbers")
	{
		if (columns->size() != 1 || columns->begin()->first != "number" || columns->begin()->second->getName() != "UInt64")
			throw Exception("Storage SystemNumbers only allows one column with name 'number' and type 'UInt64'",
				ErrorCodes::ILLEGAL_COLUMN);

		return new StorageSystemNumbers(table_name);
	}
	else if (name == "SystemOne")
	{
		if (columns->size() != 1 || columns->begin()->first != "dummy" || columns->begin()->second->getName() != "UInt8")
			throw Exception("Storage SystemOne only allows one column with name 'dummy' and type 'UInt8'",
				ErrorCodes::ILLEGAL_COLUMN);

		return new StorageSystemOne(table_name);
	}
	else
		throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
