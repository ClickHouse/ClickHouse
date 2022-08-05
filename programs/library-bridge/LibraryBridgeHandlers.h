#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/logger_useful.h>
#include "ExternalDictionaryLibraryHandler.h"


namespace DB
{


/// Handler for requests to Library Dictionary Source, returns response in RowBinary format.
/// When a library dictionary source is created, it sends 'extDict_libNew' request to library bridge (which is started on first
/// request to it, if it was not yet started). On this request a new sharedLibrayHandler is added to a
/// sharedLibraryHandlerFactory by a dictionary uuid. With 'extDict_libNew' request come: library_path, library_settings,
/// names of dictionary attributes, sample block to parse block of null values, block of null values. Everything is
/// passed in binary format and is urlencoded. When dictionary is cloned, a new handler is created.
/// Each handler is unique to dictionary.
class ExternalDictionaryLibraryBridgeRequestHandler : public HTTPRequestHandler, WithContext
{
public:
    ExternalDictionaryLibraryBridgeRequestHandler(size_t keep_alive_timeout_, ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    static constexpr inline auto FORMAT = "RowBinary";

    Poco::Logger * log;
    size_t keep_alive_timeout;
};


class ExternalDictionaryLibraryBridgeExistsHandler : public HTTPRequestHandler, WithContext
{
public:
    ExternalDictionaryLibraryBridgeExistsHandler(size_t keep_alive_timeout_, ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    const size_t keep_alive_timeout;
    Poco::Logger * log;
};

}
