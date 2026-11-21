#include <Server/WebUIRequestHandler.h>
#include <Server/HTTP/HTTPResponseHelpers.h>
#include <Server/HTTPResponseHeaderWriter.h>

#include <Common/re2.h>
#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <ClickStackResources.generated.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>
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
constexpr unsigned char resource_schema_html[] =
{
#embed "../../programs/server/schema.html"
};
constexpr unsigned char resource_processors_profile_html[] =
{
#embed "../../programs/server/processors_profile.html"
};
constexpr unsigned char resource_docs_html[] =
{
#embed "../../programs/server/docs.html"
};
constexpr unsigned char resource_marked_js[] =
{
#embed "../../programs/server/js/marked.min.js"
};
constexpr unsigned char resource_katex_js[] =
{
#embed "../../programs/server/js/katex.min.js"
};
constexpr unsigned char resource_katex_css[] =
{
#embed "../../programs/server/js/katex.min.css"
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
    auto buf = responseWriteBuffer(request, response);
    buf.get()->write(html.data(), html.size());
    buf.get()->finalize();
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
        {"/js/marked.min.js", resource_marked_js, std::size(resource_marked_js), "application/javascript; charset=UTF-8"},
        {"/js/katex.min.js", resource_katex_js, std::size(resource_katex_js), "application/javascript; charset=UTF-8"},
        {"/js/katex.min.css", resource_katex_css, std::size(resource_katex_css), "text/css; charset=UTF-8"},
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

void SchemaWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_schema_html), std::size(resource_schema_html)}, http_response_headers_override);
}

void ProcessorsProfileWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(resource_processors_profile_html), std::size(resource_processors_profile_html)}, http_response_headers_override);
}

void DocsWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(resource_docs_html), std::size(resource_docs_html));

    /// Replace links to external JavaScript/CSS files (the Marked Markdown renderer and the
    /// KaTeX math renderer) with embedded files served from the same origin.
    /// This keeps the page self-contained and, more importantly, avoids executing third-party
    /// network code in the ClickHouse HTTP origin, which handles user credentials.
    /// The original CDN links are kept in the source so the page also works when opened as a
    /// local file (file://) against a remote server, mirroring the `dashboard.html` handling.

    static re2::RE2 marked_url = R"(https://[^\s"'`]+marked[^\s"'`]*\.js)";
    RE2::Replace(&html, marked_url, "/js/marked.min.js");

    static re2::RE2 katex_js_url = R"(https://[^\s"'`]+katex[^\s"'`]*\.js)";
    RE2::Replace(&html, katex_js_url, "/js/katex.min.js");

    static re2::RE2 katex_css_url = R"(https://[^\s"'`]+katex[^\s"'`]*\.css)";
    RE2::Replace(&html, katex_css_url, "/js/katex.min.css");

    handle(request, response, html, http_response_headers_override);
}

std::optional<std::string> ClickStackUIRequestHandler::getResourcePath(const std::string & uri) const
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

    /// `Poco::URI::decode` throws `Poco::URISyntaxException` on malformed
    /// percent-encoding (e.g. lone `%`, `%X`, `%ZZ`). Without this catch the
    /// exception would unwind into the server error handler and produce a 500
    /// for any client that sends a malformed URI; returning std::nullopt lets
    /// the request handler answer with a deterministic 400.
    std::string decoded;
    try
    {
        Poco::URI::decode(std::string(path), decoded);
    }
    catch (const Poco::URISyntaxException &)
    {
        return std::nullopt;
    }

    // Handle clean URLs - map page routes to .html files
    // If path is empty or just "/", serve index.html
    if (decoded.empty())
        return std::string("index.html");

    if (decoded.find('.') != std::string::npos)
        return decoded;

    // assuming a path with no "." is an html page
    return decoded + ".html";
}

namespace
{

/// Look up `path` in the sorted embedded_resources array. Returns null if missing.
const ClickStack::EmbeddedResource * findEmbeddedResource(const std::string & resource_path)
{
    auto it = std::lower_bound(
        ClickStack::embedded_resources.begin(),
        ClickStack::embedded_resources.end(),
        resource_path,
        [](const ClickStack::EmbeddedResource & resource, const std::string & path)
        {
            return resource.path < path;
        });

    if (it == ClickStack::embedded_resources.end() || it->path != resource_path)
        return nullptr;
    return &*it;
}

/// Resolve a page request against Next.js-style dynamic routes.
/// Example: /trace/abc -> /trace/[traceId].html
const ClickStack::EmbeddedResource * resolveDynamicRoute(const std::string & resource_path)
{
    static constexpr std::string_view html_suffix = ".html";
    static constexpr std::string_view dynamic_tail_suffix = "].html";

    if (!std::string_view{resource_path}.ends_with(html_suffix))
        return nullptr;

    size_t last_slash = resource_path.rfind('/');
    std::string_view prefix = last_slash == std::string::npos
        ? std::string_view{}
        : std::string_view{resource_path}.substr(0, last_slash + 1);

    for (const auto & candidate : ClickStack::embedded_resources)
    {
        std::string_view candidate_path{candidate.path};
        if (!candidate_path.starts_with(prefix))
            continue;

        std::string_view tail = candidate_path.substr(prefix.size());
        if (tail.empty() || tail.front() != '[')
            continue;
        if (!tail.ends_with(dynamic_tail_suffix))
            continue;
        if (tail.find('/') != std::string_view::npos)
            continue;

        return &candidate;
    }

    return nullptr;
}

}

void ClickStackUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    auto resource_path_opt = getResourcePath(request.getURI());
    if (!resource_path_opt)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << "Malformed URL.\n";
        return;
    }
    const std::string & resource_path = *resource_path_opt;

    const ClickStack::EmbeddedResource * resource = findEmbeddedResource(resource_path);

    /// If the literal lookup missed and the request is for a page, try to
    /// resolve it against a Next.js dynamic-route export (`foo/[id].html`).
    /// This is what makes URLs like `/clickstack/trace/<trace-id>` serve the
    /// `trace/[traceId].html` redirect page instead of 404ing.
    if (!resource)
        resource = resolveDynamicRoute(resource_path);

    // Check if resource was found
    if (!resource)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
        return;
    }

    response.setContentType(std::string(resource->mime_type));

    // Add Content-Encoding header since all clickstack resources are pre-gzipped
    auto headers_with_encoding = http_response_headers_override;
    headers_with_encoding["Content-Encoding"] = "gzip";

    handle(request, response, {reinterpret_cast<const char *>(resource->data), resource->size}, headers_with_encoding);
}

}
