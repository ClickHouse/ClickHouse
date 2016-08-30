#include <DB/Storages/StorageCloud.h>
#include <DB/Databases/DatabaseCloud.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


StorageCloud::StorageCloud(
	DatabasePtr & database_ptr_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_), database_ptr(database_ptr_)
{
	DatabasePtr owned_db = database_ptr.lock();
	if (!owned_db)
		throw Exception("DatabaseCloud is detached", ErrorCodes::LOGICAL_ERROR);

	DatabaseCloud & db = static_cast<DatabaseCloud &>(*owned_db);
}


StoragePtr StorageCloud::create(
	DatabasePtr & database_ptr_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
{
	return make_shared(database_ptr_, name_, columns_, materialized_columns_, alias_columns_, column_defaults_);
}


}
