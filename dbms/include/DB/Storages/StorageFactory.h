#pragma once

#include <DB/Storages/IStorage.h>


namespace DB
{

using Poco::SharedPtr;


/** Позволяет создать таблицу по имени движка.
  */
class StorageFactory
{
public:
	StoragePtr get(
		const String & name,
		const String & data_path,
		const String & table_name,
		SharedPtr<NamesAndTypes> columns) const;
};

typedef SharedPtr<StorageFactory> StorageFactoryPtr;


}
