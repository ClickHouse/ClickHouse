#pragma once

#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPServerRequest.h>

namespace DB
{

HTTPServerRequest buildRequest(
    HTTPContextPtr context,
    Poco::Net::HTTPServerSession & session,
    const ProfileEvents::Event & read_event);

}
