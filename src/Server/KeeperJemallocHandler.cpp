#include <Server/KeeperJemallocHandler.h>

#if USE_NURAFT

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#include <Processors/Sources/JemallocProfileSource.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <base/scope_guard.h>
#include <filesystem>
#include <optional>
#endif

/// Reuse the server's jemalloc HTML — the page auto-adapts via window.JEMALLOC_CONFIG.
constexpr unsigned char resource_jemalloc_html[] =
{
#embed "../../programs/server/jemalloc.html"
};

namespace DB
{

void KeeperJemallocWebUIHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(resource_jemalloc_html), std::size(resource_jemalloc_html));

    constexpr std::string_view head_close = "<head>";
    auto pos = html.find(head_close);
    if (pos != std::string::npos)
        html.insert(pos + head_close.size(), "<script>window.JEMALLOC_CONFIG={mode:'keeper'}</script>");

    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
}

void KeeperJemallocRedirectHandler::handleRequest(
    HTTPServerRequest &, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    setResponseDefaultHeaders(response);
    response.redirect("/jemalloc", Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
}

#if USE_JEMALLOC

void KeeperJemallocProfileHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    Poco::URI uri(request.getURI());
    auto params = uri.getQueryParameters();

    std::string format = "collapsed";
    for (const auto & [key, value] : params)
    {
        if (key == "format")
            format = value;
    }

    if (format != "collapsed" && format != "raw")
    {
        setResponseDefaultHeaders(response);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        response.setContentType("text/plain");
        *response.send() << "Unknown format: " << format << ". Supported: collapsed, raw\n";
        return;
    }

    Jemalloc::checkProfilingEnabled();

    if (request.getMethod() == HTTPRequest::HTTP_HEAD)
    {
        setResponseDefaultHeaders(response);
        response.setContentType("text/plain; charset=UTF-8");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        response.send();
        return;
    }

    auto raw_file = std::string(Jemalloc::flushProfile("/tmp/jemalloc_keeper"));
    SCOPE_EXIT({
        std::error_code ec;
        std::filesystem::remove(raw_file, ec);
        if (ec)
            LOG_WARNING(getLogger("KeeperJemallocProfileHandler"), "Failed to remove temporary heap profile {}: {}", raw_file, ec.message());
    });

    std::string output;

    if (format == "collapsed")
    {
        output = symbolizeJemallocHeapProfileToString(raw_file, JemallocProfileFormat::Collapsed, /* symbolize_with_inline= */ false);
    }
    else
    {
        ReadBufferFromFile in(raw_file);
        readStringUntilEOF(output, in);
    }

    setResponseDefaultHeaders(response);
    response.setContentType("text/plain; charset=UTF-8");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(output.data(), output.size());
    wb.finalize();
}
catch (...)
{
    tryLogCurrentException("KeeperJemallocProfileHandler");

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << getCurrentExceptionMessage(false) << '\n';
    }
    catch (...)
    {
        LOG_ERROR(getLogger("KeeperJemallocProfileHandler"), "Cannot send exception to client");
    }
}

void KeeperJemallocStatsHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    if (request.getMethod() == HTTPRequest::HTTP_HEAD)
    {
        setResponseDefaultHeaders(response);
        response.setContentType("text/plain; charset=UTF-8");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        response.send();
        return;
    }

    auto stats = Jemalloc::getStats();

    setResponseDefaultHeaders(response);
    response.setContentType("text/plain; charset=UTF-8");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, false);
    wb.write(stats.data(), stats.size());
    wb.finalize();
}
catch (...)
{
    tryLogCurrentException("KeeperJemallocStatsHandler");

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << getCurrentExceptionMessage(false) << '\n';
    }
    catch (...)
    {
        LOG_ERROR(getLogger("KeeperJemallocStatsHandler"), "Cannot send exception to client");
    }
}

void KeeperJemallocStatusHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    Poco::JSON::Object json;
    Poco::JSON::Array errors;

    auto read_mallctl = [&]<typename T>(const char * name, std::type_identity<T>) -> std::optional<T>
    {
        try
        {
            return Jemalloc::getValue<T>(name);
        }
        catch (...)
        {
            tryLogCurrentException("KeeperJemallocStatusHandler", std::string("Failed to read mallctl '") + name + "'");
            errors.add(std::string(name));
            return std::nullopt;
        }
    };

    auto prof_enabled = read_mallctl("opt.prof", std::type_identity<bool>{});

    std::optional<bool> prof_active;
    std::optional<bool> thread_active_init;
    std::optional<size_t> lg_sample;

    if (prof_enabled.value_or(false))
    {
        prof_active = read_mallctl("prof.active", std::type_identity<bool>{});
        lg_sample = read_mallctl("prof.lg_sample", std::type_identity<size_t>{});
        try
        {
            thread_active_init = Jemalloc::getThreadProfileInitMib().getValue();
        }
        catch (...)
        {
            tryLogCurrentException("KeeperJemallocStatusHandler", "Failed to read prof.thread_active_init");
            errors.add("prof.thread_active_init");
        }
    }
    else if (prof_enabled.has_value())
    {
        prof_active = false;
        thread_active_init = false;
        lg_sample = size_t(0);
    }

    auto set_opt_bool = [&json](const char * key, const std::optional<bool> & opt)
    {
        if (opt.has_value())
            json.set(key, *opt);
        else
            json.set(key, Poco::Dynamic::Var());
    };

    set_opt_bool("prof_enabled", prof_enabled);
    set_opt_bool("prof_active", prof_active);
    set_opt_bool("thread_active_init", thread_active_init);
    if (lg_sample.has_value())
        json.set("lg_sample", static_cast<Poco::UInt64>(*lg_sample));
    else
        json.set("lg_sample", Poco::Dynamic::Var());

    if (errors.size() > 0)
        json.set("errors", errors);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    setResponseDefaultHeaders(response);
    response.setContentType("application/json");
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    auto str = oss.str();
    wb.write(str.data(), str.size());
    wb.finalize();
}
catch (...)
{
    tryLogCurrentException("KeeperJemallocStatusHandler");

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << getCurrentExceptionMessage(false) << '\n';
    }
    catch (...)
    {
        LOG_ERROR(getLogger("KeeperJemallocStatusHandler"), "Cannot send exception to client");
    }
}

#else

void KeeperJemallocNotAvailableHandler::handleRequest(
    HTTPServerRequest &, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
    response.setContentType("text/plain");
    *response.send() << "jemalloc profiling is not available in this build\n";
}

#endif

}
#endif
