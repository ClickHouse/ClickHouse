#pragma once

#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>

namespace DB
{

using ExchangeDataSources = std::unordered_map<String, std::shared_ptr<ExchangeDataSource>>;

class ExchangeManager
{
public:
    static ExchangeManager & getInstance()
    {
        static ExchangeManager exchange_mgr;
        return exchange_mgr;
    }

    static String receiverKey(const String & query_id, Int32 fragment_id, Int32 exchange_id, const String & source)
    {
        return query_id + "_" + toString(fragment_id) + "_" + source + "_" + toString(exchange_id);
    }

    std::shared_ptr<ExchangeDataSource> findExchangeDataSource(const ExchangeDataRequest & exchange_data_request);

    void registerExchangeDataSource(const ExchangeDataRequest & exchange_data_request, std::shared_ptr<ExchangeDataSource> receiver);

private:
    ExchangeDataSources exchange_data_sources;
    std::mutex mutex;
};

}

