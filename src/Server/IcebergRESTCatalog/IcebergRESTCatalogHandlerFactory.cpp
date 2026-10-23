#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

namespace DB
{

IcebergRESTCatalogHandlerFactory::IcebergRESTCatalogHandlerFactory(IServer & server_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_)
    : log(getLogger(name))
    , server(server_)
    , warehouses(std::move(warehouses_))
{
}

std::unique_ptr<HTTPRequestHandler> IcebergRESTCatalogHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP request for {}. {}", name, request.toStringForLogging());
    return std::make_unique<IcebergRESTCatalogHandler>(server, warehouses);
}

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server)
{
    auto warehouses = std::make_shared<const IcebergRESTCatalogWarehouseRegistry>(server.context());
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(server, std::move(warehouses));
}

}
