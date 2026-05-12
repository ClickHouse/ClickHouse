#include <Server/KeeperHTTPStorageHandler.h>

#if USE_NURAFT

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <IO/HTTPCommon.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
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
        = std::ranges::find_if(query_params, [](const auto & param) { return param.first == "version"; });
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

std::string getRawBytesFromRequest(HTTPServerRequest & request, const size_t max_request_size)
{
    std::string request_data;
    auto stream = request.getStream();

    if (max_request_size > 0)
    {
        LimitReadBuffer limited_stream(*stream, LimitReadBuffer::Settings{
            .read_no_more = max_request_size,
            .expect_eof = false,
            .excetion_hint = "request body is too large"});
        readStringUntilEOF(request_data, limited_stream);
    }
    else
    {
        readStringUntilEOF(request_data, *stream);
    }

    return request_data;
}

bool setErrorResponseForZKCode(const Coordination::Error error, HTTPServerResponse & response)
{
    switch (error)
    {
        case Coordination::Error::ZOK:
            return false;
        case Coordination::Error::ZNONODE:
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "Node not found.");
            *response.send() << "Requested node not found.\n";
            return true;
        case Coordination::Error::ZNODEEXISTS:
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_CONFLICT, "Node already exists.");
            *response.send() << "Node already exists.\n";
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
}

KeeperHTTPStorageHandler::KeeperHTTPStorageHandler(
    std::shared_ptr<KeeperHTTPClient> keeper_client_,
    size_t max_request_size_)
    : log(getLogger("KeeperHTTPStorageHandler"))
    , keeper_client(std::move(keeper_client_))
    , max_request_size(max_request_size_)
{
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

    if (!keeper_client->get()->exists(storage_path, &stat))
    {
        setErrorResponseForZKCode(Coordination::Error::ZNONODE, response);
        return;
    }

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
    const auto error = keeper_client->get()->tryGetChildren(storage_path, result, &stat);

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
    if (!keeper_client->get()->tryGet(storage_path, result))
    {
        setErrorResponseForZKCode(Coordination::Error::ZNONODE, response);
        return;
    }

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

    const auto error = keeper_client->get()->trySet(storage_path, getRawBytesFromRequest(request, max_request_size), maybe_request_version.value());

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("text/plain");
    *response.send() << "OK\n";
}

void KeeperHTTPStorageHandler::performZooKeeperCreateRequest(
    const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto error = keeper_client->get()->tryCreate(storage_path, getRawBytesFromRequest(request, max_request_size), zkutil::CreateMode::Persistent);

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_CREATED, "Created");
    response.setContentType("text/plain");
    response.set("Location", storage_path);
    *response.send() << "Created\n";
}

void KeeperHTTPStorageHandler::performZooKeeperRemoveRequest(
    const std::string & storage_path, const HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto maybe_request_version = getVersionFromRequest(request);
    if (!maybe_request_version.has_value())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Version parameter is not set or invalid.");
        *response.send() << "Version parameter is not set or invalid for DELETE request.\n";
        return;
    }

    const auto error = keeper_client->get()->tryRemove(storage_path, maybe_request_version.value());

    if (setErrorResponseForZKCode(error, response))
        return;

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NO_CONTENT, "No Content");
    response.send();
}

std::optional<Coordination::OpNum> KeeperHTTPStorageHandler::getOperationFromRequest(
    const HTTPServerRequest & request, HTTPServerResponse & response)
{
    const auto & method = request.getMethod();

    if (method == Poco::Net::HTTPRequest::HTTP_GET)
    {
        /// Check for ?children=true query parameter
        Poco::URI uri(request.getURI());
        const auto query_params = uri.getQueryParameters();
        const auto children_param = std::ranges::find_if(
            query_params, [](const auto & param) { return param.first == "children"; });

        if (children_param != query_params.end() && children_param->second == "true")
            return Coordination::OpNum::List;

        return Coordination::OpNum::Get;
    }
    if (method == Poco::Net::HTTPRequest::HTTP_HEAD)
        return Coordination::OpNum::Exists;
    if (method == Poco::Net::HTTPRequest::HTTP_POST)
        return Coordination::OpNum::Create;
    if (method == Poco::Net::HTTPRequest::HTTP_PUT)
        return Coordination::OpNum::Set;
    if (method == Poco::Net::HTTPRequest::HTTP_DELETE)
        return Coordination::OpNum::Remove;

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED, "Method not allowed.");
    *response.send() << "HTTP method '" << method << "' is not supported. "
                     << "Use GET, HEAD, POST, PUT, or DELETE.\n";
    return std::nullopt;
}

void KeeperHTTPStorageHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
try
{
    static constexpr auto uri_segments_prefix_length = 3;  /// /api/v1/storage

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

    const auto maybe_opnum = getOperationFromRequest(request, response);
    if (!maybe_opnum.has_value())
        return;
    const auto opnum = maybe_opnum.value();

    /// Build storage path from URL segments after /api/v1/storage
    std::string storage_path;
    for (size_t i = uri_segments_prefix_length; i < uri_segments.size(); ++i)
        storage_path += "/" + uri_segments[i];
    if (storage_path.empty())
        storage_path = "/";

    setResponseDefaultHeaders(response);

    try
    {
        performZooKeeperRequest(opnum, storage_path, request, response);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error when executing Keeper storage operation");
        response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        *response.send() << getCurrentExceptionMessage(false) << '\n';
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
