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
}

Poco::JSON::Object statToJSON(const Coordination::Stat & stat)
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
    keeper_client = zkutil::ZooKeeper::create_from_impl(
        std::make_unique<Coordination::KeeperOverDispatcher>(keeper_dispatcher, operation_timeout));
}

void KeeperHTTPStorageHandler::performZooKeeperRequest(
    Coordination::OpNum opnum, const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const
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

void KeeperHTTPStorageHandler::performZooKeeperExistsRequest(const std::string & storage_path, HTTPServerResponse & response) const
{
    Coordination::Stat stat;
    bool exits = keeper_client->exists(storage_path, &stat);

    if (!exits)
        setErrorResponseForZKCode(Coordination::Error::ZNONODE, response);

    Poco::JSON::Object response_json;
    response_json.set("stat", statToJSON(stat));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");

    *response.send() << oss.str();
}

void KeeperHTTPStorageHandler::performZooKeeperListRequest(const std::string & storage_path, HTTPServerResponse & response) const
{
    Coordination::Stat stat;
    Strings result;
    const auto error = keeper_client->tryGetChildren(storage_path, result, &stat);

    if (setErrorResponseForZKCode(error, response))
        return;

    Poco::JSON::Object response_json;
    response_json.set("child_node_names", result);
    response_json.set("stat", statToJSON(stat));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");

    *response.send() << oss.str();
}

void KeeperHTTPStorageHandler::performZooKeeperGetRequest(const std::string & storage_path, HTTPServerResponse & response) const
{
    String result;
    bool exits = keeper_client->tryGet(storage_path, result);

    if (!exits)
        setErrorResponseForZKCode(Coordination::Error::ZNONODE, response);

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/octet-stream");
    response.setContentLength(result.size());

    auto buffer = response.send();
    buffer->write(result.c_str(), result.size());
    buffer->next();
}

void KeeperHTTPStorageHandler::performZooKeeperSetRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto maybe_request_version = getVersionFromRequest(request);
    if (!maybe_request_version.has_value())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Version parameter is not set or invalid.");
        *response.send() << "Version parameter is not set or invalid for set request.\n";
        return;
    }

    const auto error = keeper_client->trySet(storage_path, getRawBytesFromRequest(request), maybe_request_version.value());

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::performZooKeeperCreateRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto error = keeper_client->tryCreate(storage_path, getRawBytesFromRequest(request), zkutil::CreateMode::Persistent);

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::performZooKeeperRemoveRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto maybe_request_version = getVersionFromRequest(request);
    if (!maybe_request_version.has_value())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Version parameter is not set or invalid.");
        *response.send() << "Version parameter is not set or invalid for remove request.\n";
        return;
    }

    const auto error = keeper_client->tryRemove(storage_path, maybe_request_version.value());

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
try
{
    static const auto uri_segments_prefix_length = 3;  /// /api/v1/storage
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
