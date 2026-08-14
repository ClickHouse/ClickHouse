#include <Server/WebUIRequestHandler.h>
#include <Server/HTTP/HTTPResponseHelpers.h>
#include <Server/HTTPResponseHeaderWriter.h>

#include <Common/Exception.h>
#include <Common/QueryScope.h>
#include <Common/quoteString.h>
#include <Common/re2.h>
#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Server/IServer.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <ClickStackResources.generated.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <array>
#include <sstream>
#include <vector>

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
constexpr unsigned char resource_docs_logo_light_svg[] =
{
#embed "../../docs/_site/logo/light.svg"
};
constexpr unsigned char resource_docs_logo_dark_svg[] =
{
#embed "../../docs/_site/logo/dark.svg"
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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

static void handle(HTTPServerRequest & request, HTTPServerResponse & response, std::string_view html,
                   std::unordered_map<String, String> http_response_headers_override = {},
                   Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK)
{
    applyHTTPResponseHeaders(response, http_response_headers_override);
    if (response.getContentType().empty())
        response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(status);
    auto buf = responseWriteBuffer(request, response);
    buf.get()->write(html.data(), html.size());
    buf.get()->finalize();
}

namespace
{

struct DocumentationRoute
{
    std::string_view type;
    std::string_view path;
};

constexpr std::array DOCUMENTATION_ROUTES =
{
    DocumentationRoute{"Function", "functions"},
    DocumentationRoute{"Aggregate Function", "aggregate-functions"},
    DocumentationRoute{"Table Function", "table-functions"},
    DocumentationRoute{"Table Engine", "table-engines"},
    DocumentationRoute{"Database Engine", "database-engines"},
    DocumentationRoute{"Data Type", "data-types"},
    DocumentationRoute{"Dictionary Layout", "dictionary-layouts"},
    DocumentationRoute{"Dictionary Source", "dictionary-sources"},
    DocumentationRoute{"Aggregate Function Combinator", "aggregate-function-combinators"},
    DocumentationRoute{"Data Skipping Index", "data-skipping-indexes"},
    DocumentationRoute{"Disk Type", "disk-types"},
    DocumentationRoute{"Setting", "settings"},
    DocumentationRoute{"MergeTree Setting", "mergetree-settings"},
    DocumentationRoute{"Server Setting", "server-settings"},
    DocumentationRoute{"Format", "formats"},
    DocumentationRoute{"Compression Codec", "compression-codecs"},
    DocumentationRoute{"Profile Event", "profile-events"},
    DocumentationRoute{"Current Metric", "current-metrics"},
    DocumentationRoute{"Asynchronous Metric", "asynchronous-metrics"},
    DocumentationRoute{"System Table", "system-tables"},
};

struct DocumentationEntity
{
    String name;
    String type;
    String description;
    String source;
};

std::string_view pathForDocumentationType(std::string_view type)
{
    for (const auto & route : DOCUMENTATION_ROUTES)
        if (route.type == type)
            return route.path;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "No documentation route is defined for entity type {}", type);
}

std::optional<std::string_view> documentationTypeForPath(std::string_view path)
{
    for (const auto & route : DOCUMENTATION_ROUTES)
        if (route.path == path)
            return route.type;
    return std::nullopt;
}

String encodeURLPathSegment(std::string_view value)
{
    String encoded;
    Poco::URI::encode(String(value), ":/?#[]@!$&'()*+,;=", encoded);
    return encoded;
}

String documentationPath(const DocumentationEntity & entity)
{
    return "/docs/" + String(pathForDocumentationType(entity.type)) + "/" + encodeURLPathSegment(entity.name);
}

String escapeHTML(std::string_view value)
{
    String result;
    result.reserve(value.size());
    for (const char character : value)
    {
        switch (character)
        {
            case '&': result += "&amp;"; break;
            case '<': result += "&lt;"; break;
            case '>': result += "&gt;"; break;
            case '"': result += "&quot;"; break;
            case '\'': result += "&#39;"; break;
            default: result += character;
        }
    }
    return result;
}

void replaceRequired(String & value, std::string_view needle, std::string_view replacement)
{
    const size_t position = value.find(needle);
    if (position == String::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The embedded documentation HTML is missing marker {}", needle);
    value.replace(position, needle.size(), replacement);
}

String executeDocumentationQuery(IServer & server, const String & query)
{
    auto query_context = Context::createCopy(server.context());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    QueryScope query_scope = QueryScope::create(query_context);

    String output;
    ReadBufferFromString input(query);
    WriteBufferFromString buffer(output);
    executeQuery(input, buffer, query_context, {}, QueryFlags{ .internal = true });
    buffer.finalize();
    return output;
}

DocumentationEntity parseDocumentationEntity(const String & json)
{
    const auto object = Poco::JSON::Parser().parse(json).extract<Poco::JSON::Object::Ptr>();
    return {
        .name = object->getValue<String>("name"),
        .type = object->getValue<String>("type"),
        .description = object->optValue<String>("description", ""),
        .source = object->optValue<String>("source", ""),
    };
}

std::optional<DocumentationEntity> getDocumentationEntity(IServer & server, std::string_view type, std::string_view name)
{
    const String query =
        "SELECT name, toString(type) AS type, description, source "
        "FROM system.documentation WHERE type = " + quoteString(type)
        + " AND name = " + quoteString(name) + " LIMIT 1 FORMAT JSONEachRow";
    String result = executeDocumentationQuery(server, query);
    if (result.empty())
        return std::nullopt;
    return parseDocumentationEntity(result);
}

std::vector<DocumentationEntity> getDocumentationIndex(IServer & server)
{
    String result = executeDocumentationQuery(
        server,
        "SELECT name, toString(type) AS type FROM system.documentation ORDER BY type, name FORMAT JSONEachRow");

    std::vector<DocumentationEntity> entities;
    ReadBufferFromString lines(result);
    while (!lines.eof())
    {
        String line;
        readStringUntilNewlineInto(line, lines);
        if (!lines.eof())
            lines.ignore();
        if (!line.empty())
            entities.push_back(parseDocumentationEntity(line));
    }
    return entities;
}

String documentationPublicOrigin(IServer & server)
{
    String configured_origin = server.context()->getServerSettings()[ServerSetting::documentation_public_url];
    if (configured_origin.empty())
        return {};

    Poco::URI uri(configured_origin);
    if ((uri.getScheme() != "http" && uri.getScheme() != "https")
        || uri.getHost().empty()
        || !uri.getUserInfo().empty()
        || (!uri.getPath().empty() && uri.getPath() != "/")
        || !uri.getRawQuery().empty()
        || !uri.getFragment().empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The `documentation_public_url` server setting must be an HTTP or HTTPS origin without a path, query, "
            "fragment, or credentials");
    }

    uri.setPath("");
    return uri.toString();
}

String documentationURL(std::string_view public_origin, std::string_view path)
{
    if (public_origin.empty())
        return String(path);
    return String(public_origin) + String(path);
}

String renderDocumentationHTML(String html, const DocumentationEntity & entity, const String & canonical_url)
{
    const String escaped_name = escapeHTML(entity.name);
    const String escaped_type = escapeHTML(entity.type);
    const String title = escaped_name + " | ClickHouse Reference";
    const String description = escaped_type + " reference for " + escaped_name + " in ClickHouse.";

    replaceRequired(html, "<title>Source Reference - ClickHouse Documentation</title>", "<title>" + title + "</title>");
    replaceRequired(html, "content=\"Search the ClickHouse source reference.\"", "content=\"" + description + "\"");
    replaceRequired(html, "href=\"\" data-doc-canonical", "href=\"" + escapeHTML(canonical_url) + "\" data-doc-canonical");

    const String initial_content =
        "<!-- DOC_INITIAL_CONTENT_BEGIN -->\n"
        "            <article id=\"doc\">\n"
        "                <div class=\"entity-type\">" + escaped_type + "</div>\n"
        "                <h1 class=\"entity-name\">" + escaped_name + "</h1>\n"
        "                <div class=\"entity-body server-rendered-markdown\">" + escapeHTML(entity.description) + "</div>\n"
        "            </article>\n"
        "            <!-- DOC_INITIAL_CONTENT_END -->";

    const size_t begin = html.find("<!-- DOC_INITIAL_CONTENT_BEGIN -->");
    const size_t end = html.find("<!-- DOC_INITIAL_CONTENT_END -->");
    if (begin == String::npos || end == String::npos || end < begin)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The embedded documentation HTML is missing its initial-content markers");
    html.replace(begin, end + String("<!-- DOC_INITIAL_CONTENT_END -->").size() - begin, initial_content);
    return html;
}

String renderDocumentationIndexHTML(String html, const String & canonical_url)
{
    replaceRequired(html, "href=\"\" data-doc-canonical", "href=\"" + escapeHTML(canonical_url) + "\" data-doc-canonical");
    return html;
}

String renderDocumentationNotFoundHTML(String html)
{
    replaceRequired(
        html,
        "<title>Source Reference - ClickHouse Documentation</title>",
        "<title>Reference page not found | ClickHouse</title>");
    const String initial_content =
        "<!-- DOC_INITIAL_CONTENT_BEGIN -->\n"
        "            <article id=\"doc\" class=\"empty\">\n"
        "                <div class=\"empty-content\"><div class=\"eyebrow\">Source reference</div>"
        "<h1>Page not found</h1><p>This reference page does not exist.</p></div>\n"
        "            </article>\n"
        "            <!-- DOC_INITIAL_CONTENT_END -->";
    const size_t begin = html.find("<!-- DOC_INITIAL_CONTENT_BEGIN -->");
    const size_t end = html.find("<!-- DOC_INITIAL_CONTENT_END -->");
    if (begin == String::npos || end == String::npos || end < begin)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The embedded documentation HTML is missing its initial-content markers");
    html.replace(begin, end + String("<!-- DOC_INITIAL_CONTENT_END -->").size() - begin, initial_content);
    return html;
}

String renderDocumentationSitemap(IServer & server, const String & origin)
{
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        "  <url><loc>" + escapeHTML(origin) + "/docs</loc></url>\n";
    for (const auto & entity : getDocumentationIndex(server))
        xml += "  <url><loc>" + escapeHTML(origin + documentationPath(entity)) + "</loc></url>\n";
    xml += "</urlset>\n";
    return xml;
}

String renderDocumentationRoutes(IServer & server)
{
    Poco::JSON::Array routes;
    for (const auto & entity : getDocumentationIndex(server))
    {
        Poco::JSON::Object route;
        route.set("name", entity.name);
        route.set("type", entity.type);
        route.set("path", documentationPath(entity));
        routes.add(route);
    }

    std::ostringstream output; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    output.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(routes, output);
    return output.str();
}

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

    static re2::RE2 docs_logo_light_url = R"(\.\./\.\./docs/_site/logo/light\.svg)";
    RE2::GlobalReplace(&html, docs_logo_light_url, "/docs/assets/logo-light.svg");

    static re2::RE2 docs_logo_dark_url = R"(\.\./\.\./docs/_site/logo/dark\.svg)";
    RE2::GlobalReplace(&html, docs_logo_dark_url, "/docs/assets/logo-dark.svg");

    String path = request.getURI();
    String query_suffix;
    if (const size_t query_position = path.find('?'); query_position != String::npos)
    {
        query_suffix = path.substr(query_position);
        path.resize(query_position);
    }

    if (path == "/docs/assets/logo-light.svg" || path == "/docs/assets/logo-dark.svg")
    {
        const bool use_dark_logo = path == "/docs/assets/logo-dark.svg";
        const unsigned char * data = use_dark_logo ? resource_docs_logo_dark_svg : resource_docs_logo_light_svg;
        const size_t size = use_dark_logo ? std::size(resource_docs_logo_dark_svg) : std::size(resource_docs_logo_light_svg);
        auto headers = http_response_headers_override;
        headers["Content-Type"] = "image/svg+xml; charset=UTF-8";
        handle(request, response, {reinterpret_cast<const char *>(data), size}, headers);
        return;
    }

    const String public_origin = documentationPublicOrigin(server);

    if (path == "/docs/sitemap.xml")
    {
        auto headers = http_response_headers_override;
        if (public_origin.empty())
        {
            headers["Content-Type"] = "text/plain; charset=UTF-8";
            handle(
                request,
                response,
                "Set the `documentation_public_url` server setting to publish the documentation sitemap.\n",
                headers,
                Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
            return;
        }
        headers["Content-Type"] = "application/xml; charset=UTF-8";
        handle(request, response, renderDocumentationSitemap(server, public_origin), headers);
        return;
    }

    if (path == "/docs/routes.json")
    {
        auto headers = http_response_headers_override;
        headers["Content-Type"] = "application/json; charset=UTF-8";
        handle(request, response, renderDocumentationRoutes(server), headers);
        return;
    }

    if (path == "/docs/")
    {
        setResponseDefaultHeaders(response);
        response.redirect("/docs" + query_suffix, Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
        return;
    }

    if (path == "/docs")
    {
        handle(
            request,
            response,
            renderDocumentationIndexHTML(html, documentationURL(public_origin, "/docs")),
            http_response_headers_override);
        return;
    }

    constexpr std::string_view prefix = "/docs/";
    if (path.starts_with(prefix))
    {
        if (path.ends_with('/'))
        {
            setResponseDefaultHeaders(response);
            response.redirect(
                path.substr(0, path.size() - 1) + query_suffix,
                Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
            return;
        }

        const std::string_view relative(path.data() + prefix.size(), path.size() - prefix.size());
        const size_t slash = relative.find('/');
        if (slash != std::string_view::npos && slash > 0 && slash + 1 < relative.size()
            && relative.find('/', slash + 1) == std::string_view::npos)
        {
            const std::string_view type_path = relative.substr(0, slash);
            const std::string_view encoded_name = relative.substr(slash + 1);
            if (const auto type = documentationTypeForPath(type_path))
            {
                String name;
                try
                {
                    Poco::URI::decode(String(encoded_name), name);
                }
                catch (const Poco::URISyntaxException &)
                {
                    handle(
                        request,
                        response,
                        renderDocumentationNotFoundHTML(html),
                        http_response_headers_override,
                        Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
                    return;
                }

                if (const auto entity = getDocumentationEntity(server, *type, name))
                {
                    const String canonical_path = documentationPath(*entity);
                    if (path != canonical_path)
                    {
                        setResponseDefaultHeaders(response);
                        response.redirect(
                            canonical_path + query_suffix,
                            Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
                        return;
                    }

                    const String canonical_url = documentationURL(public_origin, canonical_path);
                    handle(request, response, renderDocumentationHTML(html, *entity, canonical_url), http_response_headers_override);
                    return;
                }
            }
        }
    }

    handle(
        request,
        response,
        renderDocumentationNotFoundHTML(html),
        http_response_headers_override,
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
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

    if (decoded.contains('.'))
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
        if (tail.contains('/'))
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
