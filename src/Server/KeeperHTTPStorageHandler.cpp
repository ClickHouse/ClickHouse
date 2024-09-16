#include <Server/KeeperHTTPStorageHandler.h>

#if USE_NURAFT

#include "IServer.h"

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TIMEOUT_EXCEEDED;
}

Poco::JSON::Object toJSON(const Coordination::Stat & stat)
{
    Poco::JSON::Object result;
    result.set("czxid", stat.czxid);
    result.set("mzxid", stat.mzxid);
    result.set("pzxid", stat.pzxid);
    result.set("ctime", stat.ctime);
    result.set("mtime", stat.mtime);
    result.set("version", stat.version);
    result.set("cversion", stat.cversion);
    result.set("aversion", stat.aversion);
    result.set("ephemeralOwner", stat.ephemeralOwner);
    result.set("dataLength", stat.dataLength);
    result.set("numChildren", stat.numChildren);
    return result;
}

std::optional<int32_t> getVersionFromRequest(const HTTPServerRequest & request)
{
    /// we store version argument as a "version" query parameter
    Poco::URI uri(request.getURI());
    const auto query_params = uri.getQueryParameters();
    const auto version_param
        = std::find_if(query_params.begin(), query_params.begin(), [](const auto & param) { return param.first == "version"; });
    if (version_param == query_params.end())
        return std::nullopt;

    try
    {
        return parse<int32_t>(version_param->second);
    }
    catch (...)
    {
        return std::nullopt;
    }
}

std::string getRawBytesFromRequest(HTTPServerRequest & request)
{
    std::string request_data;
    char ch = 0;
    while (request.getStream().read(ch))
        request_data += ch;
    return request_data;
}

bool setErrorResponseForZKCode(Coordination::Error error, HTTPServerResponse & response)
{
    switch (error)
    {
        case Coordination::Error::ZOK:
            return false;
        case Coordination::Error::ZNONODE:
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "Node not found.");
            *response.send() << "Requested node not found.\n";
            return true;
        case Coordination::Error::ZBADVERSION:
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_CONFLICT, "Version conflict.");
            *response.send() << "Version conflict. Check the current version and try again.\n";
            return true;
        default:
            response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            *response.send() << "Keeper request finished with error: " << errorMessage(error) << ".\n";
            return true;
    }
}

KeeperHTTPStorageHandler::KeeperHTTPStorageHandler(const IServer & server_, std::shared_ptr<KeeperDispatcher> keeper_dispatcher_)
    : log(getLogger("KeeperHTTPStorageHandler"))
    , server(server_)
    , keeper_dispatcher(keeper_dispatcher_)
    , session_timeout(
          server.config().getUInt("keeper_server.http_control.storage.session_timeout", Coordination::DEFAULT_SESSION_TIMEOUT_MS)
          * Poco::Timespan::MILLISECONDS)
    , operation_timeout(
          server.config().getUInt("keeper_server.http_control.storage.operation_timeout", Coordination::DEFAULT_OPERATION_TIMEOUT_MS)
          * Poco::Timespan::MILLISECONDS)
{
}

void KeeperHTTPStorageHandler::performZooKeeperRequest(
    Coordination::OpNum opnum, const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response)
{
    switch (opnum)
    {
        case Coordination::OpNum::Exists:
            performZooKeeperExistsRequest(storage_path, response);
            return;
        case Coordination::OpNum::List:
            performZooKeeperListRequest(storage_path, response);
            return;
        case Coordination::OpNum::Get:
            performZooKeeperGetRequest(storage_path, response);
            return;
        case Coordination::OpNum::Set:
            performZooKeeperSetRequest(storage_path, request, response);
            return;
        case Coordination::OpNum::Create:
            performZooKeeperCreateRequest(storage_path, request, response);
            return;
        case Coordination::OpNum::Remove:
            performZooKeeperRemoveRequest(storage_path, request, response);
            return;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to perform ZK request for unsupported OpNum. It's a bug.");
    }
}

Coordination::ZooKeeperResponsePtr KeeperHTTPStorageHandler::awaitKeeperResponse(std::shared_ptr<Coordination::ZooKeeperRequest> request)
{
    auto response_promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
    auto response_future = response_promise->get_future();

    auto response_callback
        = [response_promise](const Coordination::ZooKeeperResponsePtr & zk_response, Coordination::ZooKeeperRequestPtr) mutable
    { response_promise->set_value(zk_response); };

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    keeper_dispatcher->registerSession(session_id, response_callback);
    SCOPE_EXIT({ keeper_dispatcher->finishSession(session_id); });

    if (request->isReadRequest())
    {
        keeper_dispatcher->putLocalReadRequest(request, session_id);
    }
    else if (!keeper_dispatcher->putRequest(request, session_id))
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", session_id);
    }

    if (response_future.wait_for(std::chrono::milliseconds(operation_timeout.totalMilliseconds())) != std::future_status::ready)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Operation timeout ({} ms) exceeded.", operation_timeout.totalMilliseconds());

    auto result = response_future.get();

    keeper_dispatcher->finishSession(session_id);
    return result;
}

void KeeperHTTPStorageHandler::performZooKeeperExistsRequest(const std::string & storage_path, HTTPServerResponse & response)
{
    Coordination::ZooKeeperExistsRequest zk_request;
    zk_request.path = storage_path;

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperExistsRequest>(std::move(zk_request)));
    auto exists_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperExistsResponse>(result_ptr);
    if (!exists_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for get operation.");

    if (setErrorResponseForZKCode(exists_result_ptr->error, response))
        return;

    Poco::JSON::Object response_json;
    response_json.set("stat", toJSON(exists_result_ptr->stat));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");

    *response.send() << oss.str();
}

void KeeperHTTPStorageHandler::performZooKeeperListRequest(const std::string & storage_path, HTTPServerResponse & response)
{
    Coordination::ZooKeeperListRequest zk_request;
    zk_request.path = storage_path;

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperListRequest>(std::move(zk_request)));
    auto list_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperListResponse>(result_ptr);
    if (!list_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for list operation.");

    if (setErrorResponseForZKCode(list_result_ptr->error, response))
        return;

    Poco::JSON::Object response_json;
    response_json.set("child_node_names", list_result_ptr->names);
    response_json.set("stat", toJSON(list_result_ptr->stat));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");

    *response.send() << oss.str();
}

void KeeperHTTPStorageHandler::performZooKeeperGetRequest(const std::string & storage_path, HTTPServerResponse & response)
{
    Coordination::ZooKeeperGetRequest zk_request;
    zk_request.path = storage_path;

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperGetRequest>(std::move(zk_request)));
    auto get_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperGetResponse>(result_ptr);
    if (!get_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for get operation.");

    if (setErrorResponseForZKCode(get_result_ptr->error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/octet-stream");
    response.setContentLength(get_result_ptr->data.size());

    response.send()->write(get_result_ptr->data.c_str(), get_result_ptr->data.size());
    response.send()->next();
}

void KeeperHTTPStorageHandler::performZooKeeperSetRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response)
{
    const auto maybe_request_version = getVersionFromRequest(request);
    if (!maybe_request_version.has_value())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Version parameter is not set or invalid.");
        *response.send() << "Version parameter is not set or invalid for set request.\n";
        return;
    }

    Coordination::ZooKeeperSetRequest zk_request;
    zk_request.path = storage_path;
    zk_request.data = getRawBytesFromRequest(request);
    zk_request.version = maybe_request_version.value();

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperSetRequest>(std::move(zk_request)));
    auto set_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperSetResponse>(result_ptr);
    if (!set_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for set operation.");

    if (setErrorResponseForZKCode(set_result_ptr->error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::performZooKeeperCreateRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response)
{
    Coordination::ZooKeeperCreateRequest zk_request;
    zk_request.path = storage_path;
    zk_request.data = getRawBytesFromRequest(request);

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperCreateRequest>(std::move(zk_request)));
    auto create_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperCreateResponse>(result_ptr);
    if (!create_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for create operation.");

    if (setErrorResponseForZKCode(create_result_ptr->error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::performZooKeeperRemoveRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response)
{
    const auto maybe_request_version = getVersionFromRequest(request);
    if (!maybe_request_version.has_value())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Version parameter is not set or invalid.");
        *response.send() << "Version parameter is not set or invalid for remove request.\n";
        return;
    }
    Coordination::ZooKeeperRemoveRequest zk_request;
    zk_request.path = storage_path;
    zk_request.version = maybe_request_version.value();

    const auto result_ptr = awaitKeeperResponse(std::make_shared<Coordination::ZooKeeperRemoveRequest>(std::move(zk_request)));
    auto remove_result_ptr = std::dynamic_pointer_cast<Coordination::ZooKeeperRemoveResponse>(result_ptr);
    if (!remove_result_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type for remove operation.");

    if (setErrorResponseForZKCode(remove_result_ptr->error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
try
{
    static const auto uri_segments_prefix_length = 3; /// /api/v1/storage
    static const std::unordered_map<std::string, Coordination::OpNum> supported_storage_operations = {
        {"exists", Coordination::OpNum::Exists},
        {"list", Coordination::OpNum::List},
        {"get", Coordination::OpNum::Get},
        {"set", Coordination::OpNum::Set},
        {"create", Coordination::OpNum::Create},
        {"remove", Coordination::OpNum::Remove},
    };

    std::vector<std::string> uri_segments;
    try
    {
        Poco::URI uri(request.getURI());
        uri.getPathSegments(uri_segments);
    }
    catch (...)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Could not parse request path.");
        *response.send() << "Could not parse request path. Check if special symbols are used.\n";
        return;
    }

    // non-strict path "/api/v1/storage" filter is already attached
    if (uri_segments.size() <= uri_segments_prefix_length)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Invalid storage request path.");
        *response.send() << "Invalid storage request path.\n";
        return;
    }

    const auto & operation_name = uri_segments[uri_segments_prefix_length];
    if (!supported_storage_operations.contains(operation_name))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Storage operation is not supported.");
        *response.send() << "Storage operation is not supported.\n";
        return;
    }
    const auto opnum = supported_storage_operations.at(operation_name);

    std::string storage_path;
    for (size_t i = uri_segments_prefix_length + 1; i < uri_segments.size(); ++i)
        storage_path += ("/" + uri_segments[i]);
    if (storage_path.empty())
        storage_path = "/";

    setResponseDefaultHeaders(response);

    if (keeper_dispatcher->isServerActive())
    {
        try
        {
            performZooKeeperRequest(opnum, storage_path, request, response);
            return;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when executing Keeper storage operation: " + operation_name);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            *response.send() << getCurrentExceptionMessage(false) << '\n';
        }
    }
    else
    {
        LOG_WARNING(log, "Ignoring user request, because the server is not active yet");
        response.setStatus(Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
        *response.send() << "Service Unavailable.\n";
    }
}
catch (...)
{
    tryLogCurrentException(log);

    try
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
        {
            /// We have not sent anything yet and we don't even know if we need to compress response.
            *response.send() << getCurrentExceptionMessage(false) << '\n';
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Cannot send exception to client");
    }
}

}
#endif
