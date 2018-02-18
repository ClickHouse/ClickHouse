#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Parsers/IAST.h>

#include <TableFunctions/getStructureOfRemoteTable.h>


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
    /// Request for a table description
    String query = "DESC TABLE " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
    NamesAndTypesList res;

    /// Send to the first any remote shard.
    const auto & shard_info = cluster.getAnyShardInfo();

    if (shard_info.isLocal())
        return context.getTable(database, table)->getColumnsList();

    auto input = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, InterpreterDescribeQuery::getSampleBlock(), context);
    input->setPoolMode(PoolMode::GET_ONE);
    input->setMainTable(QualifiedTableName{database, table});
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
