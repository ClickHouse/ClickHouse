#pragma once

#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>


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
    explicit ExternalDictionaryLibraryBridgeRequestHandler(ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    static constexpr auto FORMAT = "RowBinary";

    LoggerPtr log;
};


// Handler for checking if the external dictionary library is loaded (used for handshake)
class ExternalDictionaryLibraryBridgeExistsHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit ExternalDictionaryLibraryBridgeExistsHandler(ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};


/// Handler for requests to catboost library. The call protocol is as follows:
/// (1) Send a "catboost_GetTreeCount" request from the server to the bridge. It contains a library path (e.g /home/user/libcatboost.so) and
///     a model path (e.g. /home/user/model.bin). This loads the catboost library handler associated with the model path, then executes
///     GetTreeCount() on the library handler and sends the result back to the server.
/// (2) Send "catboost_Evaluate" from the server to the bridge. It contains a model path and the features to run the interference on. Step
///     (2) is called multiple times (once per chunk) by the server.
///
/// We would ideally like to have steps (1) and (2) in one atomic handler but can't because the evaluation on the server side is divided
/// into two dependent phases: FunctionCatBoostEvaluate::getReturnTypeImpl() and ::executeImpl(). So the model may in principle be unloaded
/// from the library-bridge between steps (1) and (2). Step (2) checks if that is the case and fails gracefully. This is okay because that
/// situation considered exceptional and rare.
///
/// An update of a model is performed by unloading it. The first call to "catboost_GetTreeCount" brings it into memory again.
///
/// Further handlers are provided for unloading a specific model, for unloading all models or for retrieving information about the loaded
/// models for display in a system view.
class CatBoostLibraryBridgeRequestHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit CatBoostLibraryBridgeRequestHandler(ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};


// Handler for pinging the library-bridge for catboost access (used for handshake)
class CatBoostLibraryBridgeExistsHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit CatBoostLibraryBridgeExistsHandler(ContextPtr context_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};

}
