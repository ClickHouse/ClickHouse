#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <Poco/Data/SessionPool.h>
#pragma GCC diagnostic pop

namespace DB
{
/** Main handler for requests to ODBC driver
  * requires connection_string and columns in request params
  * and also query in request body
  * response in RowBinary format
  */
class ODBCHandler : public Poco::Net::HTTPRequestHandler
{
public:
    using PoolPtr = std::shared_ptr<Poco::Data::SessionPool>;
    using PoolMap = std::unordered_map<std::string, PoolPtr>;

    ODBCHandler(std::shared_ptr<PoolMap> pool_map_,
        size_t keep_alive_timeout_,
        std::shared_ptr<Context> context_)
        : log(&Poco::Logger::get("ODBCHandler"))
        , pool_map(pool_map_)
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Poco::Logger * log;

    std::shared_ptr<PoolMap> pool_map;
    size_t keep_alive_timeout;
    std::shared_ptr<Context> context;

    static inline std::mutex mutex;

    PoolPtr getPool(const std::string & connection_str);
};

}
