#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>
#include <Server/IcebergRESTCatalog/InMemoryIcebergRESTCatalogStore.h>

namespace DB
{

IcebergRESTCatalogHandlerFactory::IcebergRESTCatalogHandlerFactory(IServer & server_, String warehouse_, IcebergRESTCatalogStorePtr store_)
    : log(getLogger(name))
    , server(server_)
    , warehouse(std::move(warehouse_))
    , store(std::move(store_))
{
}

std::unique_ptr<HTTPRequestHandler> IcebergRESTCatalogHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP request for {}. {}", name, request.toStringForLogging());
    return std::make_unique<IcebergRESTCatalogHandler>(server, warehouse, store);
}

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse)
{
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(server, std::move(warehouse), getSharedInMemoryIcebergRESTCatalogStore());
}

}
