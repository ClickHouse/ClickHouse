#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/TableFunctions/getStructureOfRemoteTable.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NO_REMOTE_SHARD_FOUND;
}


NamesAndTypesList getStructureOfRemoteTable(
	const Cluster & cluster,
	const std::string & database,
	const std::string & table,
	const Context & context)
{
	/// Запрос на описание таблицы
	String query = "DESC TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
	Settings settings = context.getSettings();
	NamesAndTypesList res;

	/// Отправляем на первый попавшийся удалённый шард.
	const auto & shard_info = cluster.getAnyShardInfo();

	if (shard_info.isLocal())
		return context.getTable(database, table)->getColumnsList();

	ConnectionPoolPtr pool = shard_info.pool;

	BlockInputStreamPtr input =
		std::make_shared<RemoteBlockInputStream>(
			pool.get(), query, &settings, nullptr,
			Tables(), QueryProcessingStage::Complete, context);
	input->readPrefix();

	const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

	while (Block current = input->read())
	{
		ColumnPtr name = current.getByName("name").column;
		ColumnPtr type = current.getByName("type").column;
		size_t size = name->size();

		for (size_t i = 0; i < size; ++i)
		{
			String column_name = (*name)[i].get<const String &>();
			String data_type_name = (*type)[i].get<const String &>();

			res.emplace_back(column_name, data_type_factory.get(data_type_name));
		}
	}

	return res;
}

}
