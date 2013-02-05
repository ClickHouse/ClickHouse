#include <DB/Storages/StoragePtr.h>
#include <DB/Storages/IStorage.h>
#include <Yandex/logger_useful.h>


namespace DB
{

StoragePtr::Wrapper::Wrapper() {}

StoragePtr::Wrapper::Wrapper(IStorage * s) : storage(s) {}

StoragePtr::Wrapper::~Wrapper()
{
	if (std::uncaught_exception())
	{
		try
		{
			LOG_ERROR(&Logger::get("StoragePtr"), "Maybe ignored drop table query because of uncaught exception.");
		}
		catch(...)
		{
		}
	}
	else
	{
		if (storage && storage->drop_on_destroy)
		{
			storage->dropImpl();
			
			if (Poco::File(storage->path_to_remove_on_drop).exists())
				Poco::File(storage->path_to_remove_on_drop).remove(true);
		}
	}
}

}
