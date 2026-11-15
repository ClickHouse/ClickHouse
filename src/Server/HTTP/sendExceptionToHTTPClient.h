#pragma once

namespace DB
{
class HTTPServerRequest;
class HTTPServerResponse;

void drainRequestIfNeeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept;

}
