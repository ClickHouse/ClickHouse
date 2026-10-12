#include <Storages/System/StorageSystemConnections.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/IPv6ToBinary.h>
#include <Server/ConnectionRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemConnections::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"connection_id",          std::make_shared<DataTypeUInt64>(),                                       "Unique identifier of the connection. A TCP connection gets the identifier after authentication. An HTTP connection gets the identifier on its first query."},
        {"protocol",               low_cardinality_string,                                                   "Protocol of the connection: TCP (native protocol) or HTTP."},
        {"client_address",         DataTypeFactory::instance().get("IPv6"),                                  "IP address of the client. Behind a proxy, this is the address of the client if the server trusts the PROXY protocol or the `X-Forwarded-For` header. For HTTP, this is the address of the last request."},
        {"client_port",            std::make_shared<DataTypeUInt16>(),                                       "TCP port of the client. Behind a proxy, this is the port from the PROXY protocol or the `X-Forwarded-For` header if the server trusts them. For HTTP, this is the port of the last request."},
        {"server_port",            std::make_shared<DataTypeUInt16>(),                                       "Server port that accepted the connection."},
        {"user",                   std::make_shared<DataTypeString>(),                                       "Name of the authenticated user. For an HTTP connection, this is the user of the last request. Each request on a keep-alive connection has its own authentication."},
        {"status",                 low_cardinality_string,                                                   "Status of the connection. `active`: a query runs now. `idle`: the connection is open and waits for the next query or the next HTTP request."},
        {"query_id",               std::make_shared<DataTypeString>(),                                       "Identifier of the query that runs now. Empty if the connection is idle."},
        {"client_name",            std::make_shared<DataTypeString>(),                                       "Name of the client application. A TCP client sends this name in the handshake. For HTTP, this is the `User-Agent` header of the last request."},
        {"client_version_major",   std::make_shared<DataTypeUInt64>(),                                       "Major version of the client from the TCP handshake. 0 for HTTP connections."},
        {"client_version_minor",   std::make_shared<DataTypeUInt64>(),                                       "Minor version of the client from the TCP handshake. 0 for HTTP connections."},
        {"client_version_patch",   std::make_shared<DataTypeUInt64>(),                                       "Patch version of the client from the TCP handshake. 0 for HTTP connections."},
        {"connected_time",         std::make_shared<DataTypeDateTime>(),                                     "Time when the connection started. For a TCP connection, this is the time after authentication."},
        {"last_query_time",        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Start time of the last query on this connection. NULL if the connection did not run a query."},
    };
}


void StorageSystemConnections::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// The connections show the addresses and the queries of other users.
    /// Thus, a user without the `SHOW CONNECTIONS` privilege sees only the connections of this user.
    const bool show_all_users = context->getAccess()->isGranted(AccessType::SHOW_CONNECTIONS);
    const String current_user = context->getClientInfo().current_user;

    for (const auto & conn : ConnectionRegistry::instance().list())
    {
        if (!show_all_users && conn.peer.user != current_user)
            continue;

        size_t i = 0;
        res_columns[i++]->insert(conn.connection_id);
        res_columns[i++]->insert(conn.info.protocol);
        res_columns[i++]->insertData(IPv6ToBinary(conn.peer.client_address).data(), 16);
        res_columns[i++]->insert(conn.peer.client_port);
        res_columns[i++]->insert(conn.info.server_port);
        res_columns[i++]->insert(conn.peer.user);
        res_columns[i++]->insert(conn.is_active ? "active" : "idle");
        res_columns[i++]->insert(conn.query_id);
        res_columns[i++]->insert(conn.peer.client_name);
        res_columns[i++]->insert(conn.info.client_version_major);
        res_columns[i++]->insert(conn.info.client_version_minor);
        res_columns[i++]->insert(conn.info.client_version_patch);
        res_columns[i++]->insert(static_cast<UInt32>(conn.info.connected_time));
        if (conn.last_query_time != 0)
            res_columns[i++]->insert(static_cast<UInt32>(conn.last_query_time));
        else
            res_columns[i++]->insertDefault();
    }
}

}
