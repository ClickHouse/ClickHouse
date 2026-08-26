#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>
#include <Server/IcebergRESTCatalog/InMemoryIcebergRESTCatalogStore.h>

#include <Poco/Net/HTTPRequest.h>

namespace DB
{

IcebergRESTCatalogHandlerFactory::IcebergRESTCatalogHandlerFactory(String warehouse_, IcebergRESTCatalogStorePtr store_)
    : log(getLogger(name))
    , warehouse(std::move(warehouse_))
    , store(std::move(store_))
{
}

std::unique_ptr<HTTPRequestHandler> IcebergRESTCatalogHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP request for {}. {}", name, request.toStringForLogging());

    const auto & method = request.getMethod();
    if (method == Poco::Net::HTTPRequest::HTTP_GET || method == Poco::Net::HTTPRequest::HTTP_HEAD
        || method == Poco::Net::HTTPRequest::HTTP_POST || method == Poco::Net::HTTPRequest::HTTP_DELETE)
        return std::make_unique<IcebergRESTCatalogHandler>(warehouse, store);

    return nullptr;
}

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(String warehouse)
{
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(std::move(warehouse), getSharedInMemoryIcebergRESTCatalogStore());
}

}
