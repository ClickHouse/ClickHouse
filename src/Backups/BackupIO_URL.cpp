#include <Backups/BackupIO_URL.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_http_get_redirects;
}

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace
{
    HTTPHeaderEntries makeAuthHeaders(const Poco::Net::HTTPBasicCredentials & credentials)
    {
        if (credentials.getUsername().empty())
            return {};
        Poco::Net::HTTPRequest tmp_request;
        credentials.authenticate(tmp_request);
        String auth = tmp_request.get("Authorization", "");
        if (auth.empty())
            return {};
        return {{"Authorization", auth}};
    }
}


BackupReaderURL::BackupReaderURL(
    const Poco::URI & uri_,
    const String & http_user_name,
    const String & http_password,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderURL"))
    , base_uri(uri_)
    , credentials(http_user_name, http_password)
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
    , max_redirects(context_->getSettingsRef()[Setting::max_http_get_redirects])
    , context(context_)
{
}

BackupReaderURL::~BackupReaderURL() = default;

Poco::URI BackupReaderURL::getURIForFile(const String & file_name) const
{
    Poco::URI uri = base_uri;
    String path = uri.getPath();
    if (!path.empty() && path.back() != '/')
        path += '/';
    path += file_name;
    uri.setPath(path);
    return uri;
}

bool BackupReaderURL::fileExists(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    try
    {
        auto buf = BuilderRWBufferFromHTTP(file_uri)
                       .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                       .withSettings(read_settings)
                       .withTimeouts(timeouts)
                       .withHostFilter(&context->getRemoteHostFilter())
                       .withRedirects(max_redirects)
                       .create(credentials);
        buf->getFileInfo();
        return true;
    }
    catch (const HTTPException & e)
    {
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return false;
        throw;
    }
}

UInt64 BackupReaderURL::getFileSize(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    auto buf = BuilderRWBufferFromHTTP(file_uri)
                   .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                   .withSettings(read_settings)
                   .withTimeouts(timeouts)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withRedirects(max_redirects)
                   .create(credentials);
    auto info = buf->getFileInfo();
    if (!info.file_size)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot get file size from URL: {}", file_uri.toString());
    return *info.file_size;
}

std::unique_ptr<ReadBufferFromFileBase> BackupReaderURL::readFile(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    auto buf = BuilderRWBufferFromHTTP(file_uri)
                   .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                   .withSettings(read_settings)
                   .withTimeouts(timeouts)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withRedirects(max_redirects)
                   .withDelayInit(false)
                   .create(credentials);
    return std::make_unique<ReadBufferFromFileDecorator>(std::move(buf), file_uri.toString());
}


BackupWriterURL::BackupWriterURL(
    const Poco::URI & uri_,
    const String & http_user_name,
    const String & http_password,
    const String & http_method_,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterURL"))
    , base_uri(uri_)
    , credentials(http_user_name, http_password)
    , http_method(http_method_.empty() ? Poco::Net::HTTPRequest::HTTP_PUT : http_method_)
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
    , max_redirects(context_->getSettingsRef()[Setting::max_http_get_redirects])
    , context(context_)
{
}

BackupWriterURL::~BackupWriterURL() = default;

Poco::URI BackupWriterURL::getURIForFile(const String & file_name) const
{
    Poco::URI uri = base_uri;
    String path = uri.getPath();
    if (!path.empty() && path.back() != '/')
        path += '/';
    path += file_name;
    uri.setPath(path);
    return uri;
}

bool BackupWriterURL::fileExists(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    try
    {
        auto buf = BuilderRWBufferFromHTTP(file_uri)
                       .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                       .withSettings(read_settings)
                       .withTimeouts(timeouts)
                       .withHostFilter(&context->getRemoteHostFilter())
                       .withRedirects(max_redirects)
                       .create(credentials);
        buf->getFileInfo();
        return true;
    }
    catch (const HTTPException & e)
    {
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return false;
        throw;
    }
}

UInt64 BackupWriterURL::getFileSize(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    auto buf = BuilderRWBufferFromHTTP(file_uri)
                   .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                   .withSettings(read_settings)
                   .withTimeouts(timeouts)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withRedirects(max_redirects)
                   .create(credentials);
    auto info = buf->getFileInfo();
    if (!info.file_size)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot get file size from URL: {}", file_uri.toString());
    return *info.file_size;
}

std::unique_ptr<ReadBuffer> BackupWriterURL::readFile(const String & file_name, size_t /*expected_file_size*/)
{
    auto file_uri = getURIForFile(file_name);
    return BuilderRWBufferFromHTTP(file_uri)
               .withConnectionGroup(HTTPConnectionGroupType::HTTP)
               .withSettings(read_settings)
               .withTimeouts(timeouts)
               .withHostFilter(&context->getRemoteHostFilter())
               .withRedirects(max_redirects)
               .withDelayInit(false)
               .create(credentials);
}

std::unique_ptr<WriteBuffer> BackupWriterURL::writeFile(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    return BuilderWriteBufferFromHTTP(file_uri)
               .withConnectionGroup(HTTPConnectionGroupType::HTTP)
               .withMethod(http_method)
               .withTimeouts(timeouts)
               .withAdditionalHeaders(makeAuthHeaders(credentials))
               .create();
}

void BackupWriterURL::copyFile(const String & destination, const String & source, size_t size)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {}", source, destination);
    auto in = readFile(source, size);
    auto out = writeFile(destination);
    copyData(*in, *out, size);
    out->finalize();
}

void BackupWriterURL::removeFile(const String & file_name)
{
    auto file_uri = getURIForFile(file_name);
    try
    {
        auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, file_uri, timeouts);
        Poco::Net::HTTPRequest request(
            Poco::Net::HTTPRequest::HTTP_DELETE,
            file_uri.getPathAndQuery(),
            Poco::Net::HTTPMessage::HTTP_1_1);
        if (file_uri.getPort())
            request.setHost(file_uri.getHost(), file_uri.getPort());
        else
            request.setHost(file_uri.getHost());
        if (!credentials.getUsername().empty())
            credentials.authenticate(request);
        session->sendRequest(request);
        Poco::Net::HTTPResponse response;
        auto & response_stream = session->receiveResponse(response);
        std::string ignored_body;
        Poco::StreamCopier::copyToString(response_stream, ignored_body);  /// Drain response body.
        auto status = response.getStatus();
        if (status != Poco::Net::HTTPResponse::HTTP_OK
            && status != Poco::Net::HTTPResponse::HTTP_NO_CONTENT
            && status != Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
        {
            LOG_WARNING(
                log,
                "HTTP DELETE returned unexpected status {} {} for URL {}",
                static_cast<int>(status),
                response.getReason(),
                file_uri.toString());
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to remove file {} via HTTP DELETE: {}", file_name, getCurrentExceptionMessage(false));
    }
}

}
