#include "HandlerFactory.h"
#include <Common/HTMLForm.h>
#include "Handlers.h"

#include <Dictionaries/validateODBCConnectionString.h>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
    std::string buildConnectionString(const std::string & DSN, const std::string & database)
    {
        std::stringstream ss;
        ss << "DSN=" << DSN << ";DATABASE=" << database;
        return ss.str();
    }
}
Poco::Net::HTTPRequestHandler * HandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    const auto & uri = request.getURI();
    LOG_TRACE(log, "Request URI: " + uri);

    if (uri == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        return new PingHandler(keep_alive_timeout);

    HTMLForm params(request);
    std::string DSN = params.get("DSN", "");
    std::string database = params.get("database", "");

    std::string max_block_size_str = params.get("max_block_size", "");
    std::string format = params.get("format", "RowBinary");

    std::string connection_string = buildConnectionString(DSN, database);

    LOG_TRACE(log, "Connection string:" << connection_string);

    std::shared_ptr<Poco::Data::SessionPool> pool = nullptr;
    if (!pool_map.count(connection_string))
        try
        {
            std::string validated = validateODBCConnectionString(connection_string);
            pool
                = createAndCheckResizePocoSessionPool([validated] { return std::make_shared<Poco::Data::SessionPool>("ODBC", validated); });
            pool_map[connection_string] = pool;
        }
        catch (const Exception & ex)
        {
            LOG_WARNING(log, "Connection string validation failed: " + ex.message());
        }
    else
    {
        pool = pool_map[connection_string];
    }

    size_t max_block_size = DEFAULT_BLOCK_SIZE;

    if (!max_block_size_str.empty())
        try
        {
            max_block_size = std::stoul(max_block_size_str);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        return new ODBCHandler(pool, format, max_block_size, keep_alive_timeout, context);

    return nullptr;
}
}
