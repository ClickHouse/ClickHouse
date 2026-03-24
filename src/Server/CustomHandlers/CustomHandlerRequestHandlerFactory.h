#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>

namespace DB
{

class IServer;

/// Factory that dynamically creates handlers for SQL-defined HTTP handlers.
/// On each request, looks up matching handlers from CustomHandlersFactory.
HTTPRequestHandlerFactoryPtr createCustomHandlerRequestHandlerFactory(IServer & server);

}
