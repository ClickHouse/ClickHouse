#pragma once

#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <mutex>


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

    const size_t keep_alive_timeout;
    Poco::Logger * log;
};


// Handler for checking if the external dictionary library is loaded (used for handshake)
class ExternalDictionaryLibraryBridgeExistsHandler : public HTTPRequestHandler, WithContext
{
public:
    ExternalDictionaryLibraryBridgeExistsHandler(size_t keep_alive_timeout_, ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    const size_t keep_alive_timeout;
    Poco::Logger * log;
};


/// Handler for requests to catboost library. The call protocol is as follows:
/// (1) Send a "catboost_GetTreeCount" request from the server to the bridge, containing a library path (e.g /home/user/libcatboost.so) and
///     a model path (e.g. /home/user/model.bin). Rirst, this unloads the catboost library handler associated to the model path (if it was
///     loaded), then loads the catboost library handler associated to the model path, then executes GetTreeCount() on the library handler
///     and finally sends the result back to the server.
///     Step (1) is called once by the server from FunctionCatBoostEvaluate::getReturnTypeImpl(). The library path handler is unloaded in
///     the beginning because it contains state which may no longer be valid if the user runs catboost("/path/to/model.bin", ...) more than
///     once and if "model.bin" was updated in between.
/// (2) Send "catboost_Evaluate" from the server to the bridge, containing the model path and the features to run the interference on.
///     Step (2) is called multiple times (once per chunk) by the server from function FunctionCatBoostEvaluate::executeImpl(). The library
///     handler for the given model path is expected to be already loaded by Step (1).
class CatBoostLibraryBridgeRequestHandler : public HTTPRequestHandler, WithContext
{
public:
    CatBoostLibraryBridgeRequestHandler(size_t keep_alive_timeout_, ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    std::mutex mutex;
    const size_t keep_alive_timeout;
    Poco::Logger * log;
};


// Handler for pinging the library-bridge for catboost access (used for handshake)
class CatBoostLibraryBridgeExistsHandler : public HTTPRequestHandler, WithContext
{
public:
    CatBoostLibraryBridgeExistsHandler(size_t keep_alive_timeout_, ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    const size_t keep_alive_timeout;
    Poco::Logger * log;
};

}
