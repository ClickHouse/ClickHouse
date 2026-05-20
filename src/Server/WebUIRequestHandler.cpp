#include <Server/WebUIRequestHandler.h>
#include <Server/HTTPResponseHeaderWriter.h>

#include <Common/re2.h>
#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <ClickStackResources.generated.h>
#include <SQLConsoleResources.generated.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>


/// Embedded HTML pages
constexpr unsigned char resource_play_html[] =
{
#embed "../../programs/server/play.html"
};
constexpr unsigned char resource_dashboard_html[] =
{
#embed "../../programs/server/dashboard.html"
};
constexpr unsigned char resource_uplot_js[] =
{
#embed "../../programs/server/js/uplot.js"
};
constexpr unsigned char resource_lz_string_js[] =
{
#embed "../../programs/server/js/lz-string.js"
};
constexpr unsigned char resource_xterm_js[] =
{
#embed "../../programs/server/js/xterm.min.js"
};
constexpr unsigned char resource_xterm_css[] =
{
#embed "../../programs/server/js/xterm.min.css"
};
constexpr unsigned char resource_addon_fit_js[] =
{
#embed "../../programs/server/js/addon-fit.min.js"
};
constexpr unsigned char resource_addon_web_links_js[] =
{
#embed "../../programs/server/js/addon-web-links.min.js"
};
constexpr unsigned char resource_viz_standalone_js[] =
{
#embed "../../programs/server/js/viz-standalone.js"
};
constexpr unsigned char resource_binary_html[] =
{
#embed "../../programs/server/binary.html"
};
constexpr unsigned char resource_merges_html[] =
{
#embed "../../programs/server/merges.html"
};
constexpr unsigned char resource_jemalloc_html[] =
{
#embed "../../programs/server/jemalloc.html"
};
constexpr unsigned char resource_processors_profile_html[] =
{
#embed "../../programs/server/processors_profile.html"
};


namespace DB
{

static void handle(HTTPServerRequest & request, HTTPServerResponse & response, std::string_view html,
                   std::unordered_map<String, String> http_response_headers_override = {})
{
    applyHTTPResponseHeaders(response, http_response_headers_override);
    if (response.getContentType().empty())
        response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
}

template <typename EmbeddedResources>
const typename EmbeddedResources::value_type * findEmbeddedResource(const EmbeddedResources & resources, const std::string & resource_path)
{
    auto it = std::lower_bound(
        resources.begin(),
        resources.end(),
        resource_path,
        [](const auto & resource, const std::string & path)
        {
            return resource.path < path;
        });

    if (it == resources.end() || it->path != resource_path)
        return nullptr;

    return &*it;
}

template <typename EmbeddedResource>
void handleCompressedEmbeddedResource(
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    const EmbeddedResource & resource,
    const std::unordered_map<String, String> & http_response_headers_override)
{
    response.setContentType(std::string(resource.mime_type));

    auto headers_with_encoding = http_response_headers_override;
    headers_with_encoding["Content-Encoding"] = "gzip";

    handle(request, response, {reinterpret_cast<const char *>(resource.data), resource.size}, headers_with_encoding);
}

void PlayWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_play_html), std::size(resource_play_html)}, http_response_headers_override);
}

void DashboardWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(resource_dashboard_html), std::size(resource_dashboard_html));

    /// Replace a link to external JavaScript file to embedded file.
    /// This allows to open the HTML without running a server and to host it on server.
    /// Note: we can embed the JavaScript file inline to the HTML,
    /// but we don't do it to keep the "view-source" perfectly readable.

    static re2::RE2 uplot_url = R"(https://[^\s"'`]+u[Pp]lot[^\s"'`]*\.js)";
    RE2::Replace(&html, uplot_url, "/js/uplot.js");

    static re2::RE2 lz_string_url = R"(https://[^\s"'`]+lz-string[^\s"'`]*\.js)";
    RE2::Replace(&html, lz_string_url, "/js/lz-string.js");

    handle(request, response, html, http_response_headers_override);
}

void BinaryWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_binary_html), std::size(resource_binary_html)}, http_response_headers_override);
}

void MergesWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_merges_html), std::size(resource_merges_html)}, http_response_headers_override);
}

void JavaScriptWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    struct Resource { const char * path; const unsigned char * data; size_t size; const char * content_type; };
    const Resource resources[] = {
        {"/js/uplot.js", resource_uplot_js, std::size(resource_uplot_js), "application/javascript; charset=UTF-8"},
        {"/js/lz-string.js", resource_lz_string_js, std::size(resource_lz_string_js), "application/javascript; charset=UTF-8"},
        {"/js/xterm.min.js", resource_xterm_js, std::size(resource_xterm_js), "application/javascript; charset=UTF-8"},
        {"/js/xterm.min.css", resource_xterm_css, std::size(resource_xterm_css), "text/css; charset=UTF-8"},
        {"/js/addon-fit.min.js", resource_addon_fit_js, std::size(resource_addon_fit_js), "application/javascript; charset=UTF-8"},
        {"/js/addon-web-links.min.js", resource_addon_web_links_js, std::size(resource_addon_web_links_js), "application/javascript; charset=UTF-8"},
        {"/js/viz-standalone.js", resource_viz_standalone_js, std::size(resource_viz_standalone_js), "application/javascript; charset=UTF-8"},
    };

    for (const auto & resource : resources)
    {
        if (request.getURI() == resource.path)
        {
            auto headers = http_response_headers_override;
            if (resource.content_type)
                headers["Content-Type"] = resource.content_type;
            handle(request, response, {reinterpret_cast<const char *>(resource.data), resource.size}, headers);
            return;
        }
    }

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    *response.send() << "Not found.\n";
}

void JemallocWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_jemalloc_html), std::size(resource_jemalloc_html)}, http_response_headers_override);
}

void ProcessorsProfileWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_processors_profile_html), std::size(resource_processors_profile_html)}, http_response_headers_override);
}

std::string ClickStackUIRequestHandler::getResourcePath(const std::string & uri) const
{
    std::string_view path = uri;
    if (path.starts_with("/clickstack"))
        path.remove_prefix(11); // length of "/clickstack"

    if (!path.empty() && path[0] == '/')
        path.remove_prefix(1);

    // Remove query parameters and fragments
    auto query_pos = path.find('?');
    if (query_pos != std::string_view::npos)
        path = path.substr(0, query_pos);

    auto fragment_pos = path.find('#');
    if (fragment_pos != std::string_view::npos)
        path = path.substr(0, fragment_pos);

    // Remove trailing slash
    if (!path.empty() && path.back() == '/')
        path.remove_suffix(1);

    // Handle clean URLs - map page routes to .html files
    // If path is empty or just "/", serve index.html
    if (path.empty())
        return "index.html";

    std::string path_str(path);
    if (path_str.find('.') != std::string::npos)
        return path_str;

    // assuming a path with no "." is an html page
    return path_str + ".html";
}

void ClickStackUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string resource_path = getResourcePath(request.getURI());

    const auto * resource = findEmbeddedResource(ClickStack::embedded_resources, resource_path);
    if (!resource)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
        return;
    }

    handleCompressedEmbeddedResource(request, response, *resource, http_response_headers_override);
}

std::string SQLConsoleUIRequestHandler::getResourcePath(const std::string & uri) const
{
    std::string_view path = uri;
    if (path.starts_with("/ui"))
        path.remove_prefix(3); // length of "/ui"

    if (!path.empty() && path[0] == '/')
        path.remove_prefix(1);

    auto query_pos = path.find('?');
    if (query_pos != std::string_view::npos)
        path = path.substr(0, query_pos);

    auto fragment_pos = path.find('#');
    if (fragment_pos != std::string_view::npos)
        path = path.substr(0, fragment_pos);

    if (!path.empty() && path.back() == '/')
        path.remove_suffix(1);

    if (path.empty())
        return "index.html";

    return std::string(path);
}

void SQLConsoleUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string resource_path = getResourcePath(request.getURI());

    if (const auto * resource = findEmbeddedResource(SQLConsole::embedded_resources, resource_path))
    {
        handleCompressedEmbeddedResource(request, response, *resource, http_response_headers_override);
        return;
    }

    if (resource_path.find('.') == std::string::npos)
    {
        if (const auto * resource = findEmbeddedResource(SQLConsole::embedded_resources, "index.html"))
        {
            handleCompressedEmbeddedResource(request, response, *resource, http_response_headers_override);
            return;
        }
    }

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    *response.send() << "Not found.\n";
}

}
