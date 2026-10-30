#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Common/Exception.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace ServerSetting
{
    extern const ServerSettingsBool allow_experimental_iceberg_rest_catalog;
}

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
    if (!server.context()->getServerSettings()[ServerSetting::allow_experimental_iceberg_rest_catalog])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "The Iceberg REST catalog server is experimental. Enable the server setting `allow_experimental_iceberg_rest_catalog` to use it");

    auto warehouses = std::make_shared<IcebergRESTCatalogWarehouseRegistry>(server.context());
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(server, std::move(warehouses));
}

}
