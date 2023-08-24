#pragma once

#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>

namespace DB
{

using ExchangeDataSources = std::unordered_map<String, std::shared_ptr<ExchangeDataSource>>;

using QueryExchangeDataSources = std::unordered_map<String, ExchangeDataSources>;

class ExchangeManager
{
public:
    static ExchangeManager & getInstance()
    {
        static ExchangeManager exchange_mgr;
        return exchange_mgr;
    }

    static String receiverKey(Int32 fragment_id, Int32 exchange_id, const String & source)
    {
        return toString(fragment_id) + "_" + source + "_" + toString(exchange_id);
    }

    std::shared_ptr<ExchangeDataSource> findExchangeDataSource(const ExchangeDataRequest & exchange_data_request);

    void registerExchangeDataSource(const ExchangeDataRequest & exchange_data_request, std::shared_ptr<ExchangeDataSource> receiver);

    void removeExchangeDataSources(const String & query_id);

private:
    QueryExchangeDataSources query_exchange_data_sources;
    std::mutex mutex;
};

}

