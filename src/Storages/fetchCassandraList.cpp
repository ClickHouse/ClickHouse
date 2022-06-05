// #pragma once

#include <Common/config.h>

#if USE_CASSANDRA
#include <Processors/Transforms/CassandraSource.h>
#include <Storages/StorageCassandra.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}   


static DataTypePtr convertCassandraDataType(String type)
{
    DataTypePtr res;
    type = Poco::toLower(type);

    if (type == "bigint" || type == "counter" || type == "duration" || type == "varint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "double" || type == "decimal")
        res = std::make_shared<DataTypeFloat64>();
    else if (type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (type == "int")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type == "tinyint")
        res = std::make_shared<DataTypeInt8>();
    else if (type == "ascii" || type == "date" || type == "inet" || 
    type == "text" || type == "time" || type == "timestamp" || type == "varchar" || 
    type == "blob")
        res = std::make_shared<DataTypeString>();
    else if (type == "timeuuid" || type == "uuid")
        res = std::make_shared<DataTypeInt128>();
    else
        throw Exception("UNKNOWN_TYPE", ErrorCodes::UNKNOWN_TYPE);
    return res;
}

CassSessionShared getSession(const CassClusterPtr & cluster) 
{
    std::mutex connect_mutex;
    CassSessionWeak maybe_session;
    auto session = maybe_session.lock();
    if (session)
        return session;
    std::lock_guard lock(connect_mutex);
    session = maybe_session.lock();
    if (session)
        return session;
    session = std::make_shared<CassSessionPtr>();
    CassFuturePtr future = cass_session_connect(*session, cluster);
    cassandraWaitAndCheck(future);
    maybe_session = session;
    return session;
}


std::map<String, ColumnsDescription> fetchTablesCassandra(
    String database,
    String table,
    UInt16 port,
    String host,
    String username,
    String password,
    String consist)
{
    auto columns = NamesAndTypesList();

    String query = fmt::format(
        "select column_name, type from system_schema.columns where keyspace_name in ('{}') and table_name in ('{}');"
        , database
        , table
    );

    std::map<String, ColumnsDescription> tables_and_columns;
    CassClusterPtr cluster;
    cassandraCheck(cass_cluster_set_contact_points(cluster, host.c_str()));
    if (port)
        cassandraCheck(cass_cluster_set_port(cluster, port));
    cass_cluster_set_credentials(cluster, username.c_str(), password.c_str());
    CassConsistency consistency = StorageCassandra::nameConsistency(consist); 
    cassandraCheck(cass_cluster_set_consistency(cluster, consistency));

    CassSessionShared session = getSession(cluster);


    Block sample_block;
    sample_block.insert({std::make_shared<DataTypeString>(), "column_name"});
    sample_block.insert({std::make_shared<DataTypeString>(), "type"});


    auto result = std::make_unique<CassandraSource>(session, query, sample_block, 100000);

    QueryPipeline pipeline(std::move(result));

    Block block;

    PullingPipelineExecutor executor(pipeline);
    while (executor.pull(block))
    {
        const auto & column_name = *block.getByPosition(0).column;
        const auto & column_type = * block.getByPosition(1).column;

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            ColumnDescription column_desc(
                column_name[i].safeGet<String>(),
                convertCassandraDataType(
                    column_type[i].safeGet<String>()
                )
            );
            tables_and_columns[table].add(column_desc);
        }
    }
    return tables_and_columns;
}

}

#endif
