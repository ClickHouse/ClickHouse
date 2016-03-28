#include <DB/Storages/StorageCloud.h>
#include <DB/Databases/DatabaseCloud.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


void StorageCloud::drop()
{
	DatabasePtr owned_db = database.lock();
	if (!owned_db)
		throw Exception("DatabaseCloud is detached", ErrorCodes::LOGICAL_ERROR);

	DatabaseCloud & db = static_cast<DatabaseCloud &>(*owned_db);

	
}

}
