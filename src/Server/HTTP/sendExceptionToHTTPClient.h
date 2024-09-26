#pragma once

namespace DB
{
class HTTPServerRequest;
class HTTPServerResponse;

void drainRequstIfNeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept;

}
