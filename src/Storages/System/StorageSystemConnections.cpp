#include <Storages/System/StorageSystemConnections.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/IPv6ToBinary.h>
#include <Server/ConnectionRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemConnections::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"connection_id",          std::make_shared<DataTypeUInt64>(),                                                    "Unique identifier of the connection, assigned when the connection is established."},
        {"protocol",               low_cardinality_string,                                                                "Protocol used by the connection: TCP (native protocol) or HTTP."},
        {"client_address",         DataTypeFactory::instance().get("IPv6"),                                               "IP address of the client."},
        {"client_port",            std::make_shared<DataTypeUInt16>(),                                                    "TCP port of the client."},
        {"server_port",            std::make_shared<DataTypeUInt16>(),                                                    "Server port that accepted the connection."},
        {"user",                   std::make_shared<DataTypeString>(),                                                    "Name of the authenticated user."},
        {"status",                 low_cardinality_string,                                                                "Connection status: active means a query is currently being executed, idle means the connection is open but waiting for the next query (TCP only)."},
        {"query_id",               std::make_shared<DataTypeString>(),                                                    "Identifier of the query currently being executed. Empty when the connection is idle."},
        {"client_name",            std::make_shared<DataTypeString>(),                                                    "Name of the client application, as reported during the TCP handshake. Empty for HTTP connections."},
        {"client_version_major",   std::make_shared<DataTypeUInt64>(),                                                    "Major version of the client, as reported during the TCP handshake."},
        {"client_version_minor",   std::make_shared<DataTypeUInt64>(),                                                    "Minor version of the client, as reported during the TCP handshake."},
        {"client_version_patch",   std::make_shared<DataTypeUInt64>(),                                                    "Patch version of the client, as reported during the TCP handshake."},
        {"connected_time",         std::make_shared<DataTypeDateTime>(),                                                  "Time at which the connection was established."},
        {"last_query_time",        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),              "Time at which the most recent query on this connection started. NULL if no query has been executed yet."},
    };
}


void StorageSystemConnections::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto connections = ConnectionRegistry::instance().list();

    for (const auto & conn : connections)
    {
        size_t i = 0;
        res_columns[i++]->insert(conn.connection_id);
        res_columns[i++]->insert(conn.protocol);
        res_columns[i++]->insertData(IPv6ToBinary(conn.client_address).data(), 16);
        res_columns[i++]->insert(conn.client_port);
        res_columns[i++]->insert(conn.server_port);
        res_columns[i++]->insert(conn.user);
        res_columns[i++]->insert(conn.status);
        res_columns[i++]->insert(conn.query_id);
        res_columns[i++]->insert(conn.client_name);
        res_columns[i++]->insert(conn.client_version_major);
        res_columns[i++]->insert(conn.client_version_minor);
        res_columns[i++]->insert(conn.client_version_patch);
        res_columns[i++]->insert(static_cast<UInt32>(conn.connected_time));
        if (conn.last_query_time != 0)
            res_columns[i++]->insert(static_cast<UInt32>(conn.last_query_time));
        else
            res_columns[i++]->insert(Field{});
    }
}

}
