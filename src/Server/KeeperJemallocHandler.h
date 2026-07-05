#pragma once

#include "config.h"

#if USE_NURAFT

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Serves the static jemalloc.html page at GET /jemalloc
class KeeperJemallocWebUIHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

/// Redirects /jemalloc/ → /jemalloc (HTTP 301).  Always registered.
class KeeperJemallocRedirectHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

#if USE_JEMALLOC

/// GET /jemalloc/profile?format={collapsed|raw} — heap profile dump
class KeeperJemallocProfileHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

/// GET /jemalloc/stats — raw malloc_stats_print output
class KeeperJemallocStatsHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

/// GET /jemalloc/status — profiling state as JSON
class KeeperJemallocStatusHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

#else

/// Stub handler for non-jemalloc builds; returns HTTP 501 for all API paths.
class KeeperJemallocNotAvailableHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

#endif

}

#endif
