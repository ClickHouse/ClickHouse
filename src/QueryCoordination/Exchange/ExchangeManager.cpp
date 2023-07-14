#include <QueryCoordination/Exchange/ExchangeManager.h>
#include <QueryCoordination/Fragments/DistributedFragment.h>

namespace DB
{

std::shared_ptr<ExchangeDataSource> ExchangeManager::findExchangeDataSource(const ExchangeDataRequest & exchange_data_request)
{
    std::lock_guard lock(mutex);
    const auto & receiver_key = ExchangeManager::receiverKey(
        exchange_data_request.query_id,
        exchange_data_request.fragment_id,
        exchange_data_request.exchange_id,
        exchange_data_request.from_host);

    auto it = exchange_data_sources.find(receiver_key);

    if (it == exchange_data_sources.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found exchange data receiver {}", exchange_data_request.toString());

    return it->second;
}

void ExchangeManager::registerExchangeDataSource(const ExchangeDataRequest & exchange_data_request, std::shared_ptr<ExchangeDataSource> receiver)
{
    std::lock_guard lock(mutex);
    const auto & receiver_key = ExchangeManager::receiverKey(
        exchange_data_request.query_id,
        exchange_data_request.fragment_id,
        exchange_data_request.exchange_id,
        exchange_data_request.from_host);

    exchange_data_sources.emplace(receiver_key, receiver);
}

}
