#include "ColumnInfoHandler.h"

#if USE_ODBC

#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Server/HTTP/HTMLForm.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/NumberParser.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/BridgeProtocolVersion.h>
#include <Common/quoteString.h>
#include "getIdentifierQuote.h"
#include "validateODBCConnectionString.h"
#include "ODBCPooledConnectionFactory.h"

#include <sql.h>
#include <sqlext.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 odbc_bridge_connection_pool_size;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    DataTypePtr getDataType(SQLSMALLINT type)
    {
        const auto & factory = DataTypeFactory::instance();

        switch (type)
        {
            case SQL_TINYINT:
                return factory.get("Int8");
            case SQL_INTEGER:
                return factory.get("Int32");
            case SQL_SMALLINT:
                return factory.get("Int16");
            case SQL_BIGINT:
                return factory.get("Int64");
            case SQL_FLOAT:
                return factory.get("Float64");
            case SQL_REAL:
                return factory.get("Float32");
            case SQL_DOUBLE:
                return factory.get("Float64");
            case SQL_DATETIME:
                return factory.get("DateTime");
            case SQL_TYPE_TIMESTAMP:
                return factory.get("DateTime");
            case SQL_TYPE_DATE:
                return factory.get("Date");
            default:
                return factory.get("String");
        }
    }
}


void ODBCColumnsInfoHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    HTMLForm params(getContext()->getSettingsRef(), request, request.getStream());
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    auto process_error = [&response, this](const std::string & message)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << message << '\n';
        LOG_WARNING(log, fmt::runtime(message));
    };

    size_t version;

    if (!params.has("version"))
        version = 0; /// assumed version for too old servers which do not send a version
    else
    {
        String version_str = params.get("version");
        if (!tryParse(version, version_str))
        {
            process_error("Unable to parse 'version' string in request URL: '" + version_str + "' Check if the server and library-bridge have the same version.");
            return;
        }
    }

    if (version != XDBC_BRIDGE_PROTOCOL_VERSION)
    {
        /// backwards compatibility is considered unnecessary for now, just let the user know that the server and the bridge must be upgraded together
        process_error("Server and library-bridge have different versions: '" + std::to_string(version) + "' vs. '" + std::to_string(LIBRARY_BRIDGE_PROTOCOL_VERSION) + "'");
        return;
    }

    if (!params.has("table"))
    {
        process_error("No 'table' param in request URL");
        return;
    }

    if (!params.has("connection_string"))
    {
        process_error("No 'connection_string' in request URL");
        return;
    }

    std::string schema_name;
    std::string table_name = params.get("table");
    std::string connection_string = params.get("connection_string");

    if (params.has("schema"))
        schema_name = params.get("schema");

    LOG_TRACE(log, "Got connection str '{}'", connection_string);

    try
    {
        const bool external_table_functions_use_nulls = Poco::NumberParser::parseBool(params.get("external_table_functions_use_nulls", "false"));

        auto connection_holder = ODBCPooledConnectionFactory::instance().get(
            validateODBCConnectionString(connection_string), getContext()->getSettingsRef()[Setting::odbc_bridge_connection_pool_size]);

        /// In XDBC tables it is allowed to pass either database_name or schema_name in table definion, but not both of them.
        /// They both are passed as 'schema' parameter in request URL, so it is not clear whether it is database_name or schema_name passed.
        /// If it is schema_name then we know that database is added in odbc.ini. But if we have database_name as 'schema',
        /// it is not guaranteed. For nanodbc database_name must be either in odbc.ini or passed as catalog_name.
        auto get_columns = [&](nanodbc::connection & connection)
        {
            nanodbc::catalog catalog(connection);
            std::string catalog_name;

            nanodbc::catalog::tables tables = catalog.find_tables(table_name, /* type = */ "", /* schema = */ "", /* catalog = */ schema_name);
            if (tables.next())
            {
                catalog_name = tables.table_catalog();
                /// `tables.next()` call is mandatory to drain the iterator before next operation and avoid "Invalid cursor state"
                if (tables.next())
                    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Driver returned more than one table for '{}': '{}' and '{}'",
                                    table_name, catalog_name, tables.table_schema());
                LOG_TRACE(log, "Will fetch info for table '{}.{}'", catalog_name, table_name);
                return catalog.find_columns(/* column = */ "", table_name, /* schema = */ "", catalog_name);
            }

            tables = catalog.find_tables(table_name, /* type = */ "", /* schema = */ schema_name);
            if (tables.next())
            {
                catalog_name = tables.table_catalog();
                /// `tables.next()` call is mandatory to drain the iterator before next operation and avoid "Invalid cursor state"
                if (tables.next())
                    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Driver returned more than one table for '{}': '{}' and '{}'",
                                    table_name, catalog_name, tables.table_schema());
                LOG_TRACE(log, "Will fetch info for table '{}.{}.{}'", catalog_name, schema_name, table_name);
                return catalog.find_columns(/* column = */ "", table_name, schema_name, catalog_name);
            }

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} not found", schema_name.empty() ? table_name : schema_name + '.' + table_name);
        };

        nanodbc::catalog::columns columns_definition = execute<nanodbc::catalog::columns>(
                    std::move(connection_holder),
                    [&](nanodbc::connection & connection) { return get_columns(connection); });

        NamesAndTypesList columns;
        while (columns_definition.next())
        {
            SQLSMALLINT type = columns_definition.sql_data_type();
            std::string column_name = columns_definition.column_name();

            bool is_nullable = columns_definition.nullable() == SQL_NULLABLE;

            auto column_type = getDataType(type);

            if (external_table_functions_use_nulls && is_nullable == SQL_NULLABLE)
                column_type = std::make_shared<DataTypeNullable>(column_type);

            columns.emplace_back(column_name, std::move(column_type));
        }

        /// Usually this should not happen, since in case of table does not
        /// exists, the call should be succeeded.
        /// However it is possible sometimes because internally there are two
        /// queries in ClickHouse ODBC bridge:
        /// - system.tables
        /// - system.columns
        /// And if between this two queries the table will be removed, them
        /// there will be no columns
        ///
        /// Also sometimes system.columns can return empty result because of
        /// the cached value of total tables to scan.
        if (columns.empty())
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Columns definition was not returned");

        WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD);
        try
        {
            writeStringBinary(columns.toString(), out);
            out.finalize();
        }
        catch (...)
        {
            out.finalize();
        }
    }
    catch (...)
    {
        process_error("Error getting columns from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}

}

#endif
