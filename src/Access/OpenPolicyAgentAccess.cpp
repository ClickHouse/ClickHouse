#include <Access/HTTPAuthClient.h>
#include <Access/OpenPolicyAgentAccess.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/config_version.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <cassert>


namespace DB
{
namespace ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int NOT_IMPLEMENTED;
}

namespace
{

class OPAResponseParser
{
public:
    using Result = bool;
    static constexpr const char * granted_json_field = "result";

    bool parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const
    {
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
        {
            LOG_DEBUG(getLogger("OPA"), "HTTP status: {}", static_cast<UInt32>(response.getStatus()));
            return false;
        }

        if (!body_stream)
        {
            LOG_DEBUG(getLogger("OPA"), "No response body");
            return false;
        }

        String error;
        try
        {
            Poco::JSON::Parser parser;
            const Poco::Dynamic::Var json = parser.parse(*body_stream);
            if (json.isEmpty())
            {
                LOG_DEBUG(getLogger("OPA"), "Response is not a JSON");
                return false;
            }

            const Poco::JSON::Object::Ptr & obj = json.extract<Poco::JSON::Object::Ptr>();
            if (!obj)
            {
                LOG_DEBUG(getLogger("OPA"), "Wrong root JSON object");
                return false;
            }

            const Poco::Dynamic::Var allow = obj->get(granted_json_field);
            return !allow.isEmpty() && allow.convert<bool>();
        }
        catch (const std::exception & ex)
        {
            error = ex.what();
        }

        if (!error.empty())
            LOG_DEBUG(getLogger("OPA"), "Error: {}", error);
        return false;
    }
};

String makeAccessJSON(const AccessRightsElement & el, UserPtr user)
{
    Poco::JSON::Object json;
    Poco::JSON::Object input;
    {
        Poco::JSON::Object context;
        {
            context.set("ClickHouseVersion", VERSION_STRING);

            Poco::JSON::Object identity;
            {
                if (user)
                    identity.set("user", user->getName());
            }
            context.set("identity", identity);
        }
        input.set("context", context);

        if (!el.access_flags.isEmpty())
        {
            Poco::JSON::Array grants;
            for (auto grant : el.access_flags.toAccessTypes())
                grants.add(toString(grant));
            input.set("grants", grants);
        }

        Poco::JSON::Object access;
        {
            if (!el.database.empty())
                access.set("database", el.database);
            if (!el.table.empty())
                access.set("table", el.table);
            if (!el.columns.empty())
            {
                Poco::JSON::Array columns;
                for (auto & column_name : el.columns)
                    columns.add(column_name);
                access.set("columns", columns);
            }
        }
        input.set("access", access);
    }
    json.set("input", input);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

}

std::shared_ptr<const OpenPolicyAgentAccess> OpenPolicyAgentAccess::create(ContextPtr context)
{
    static const String prefix = "open_policy_agent";

    const auto & config = context->getConfigRef();
    if (!config.has(prefix))
    {
        LOG_INFO(getLogger("OPA"), "No Open Policy Agent in config. Disabled.");
        return {};
    }

    String url;
    if (config.has(prefix + ".address"))
        url = config.getString(prefix + ".address");

    String token;
    if (config.has(prefix + ".token"))
        token = config.getString(prefix + ".token");

    String role_name;
    if (config.has(prefix + ".role"))
        role_name = config.getString(prefix + ".role");

    if (role_name.empty() || url.empty())
    {
        LOG_WARNING(getLogger("OPA"), "No role or address for Open Policy Agent in config. Disabled.");
        return {};
    }

    return std::make_shared<const OpenPolicyAgentAccess>(role_name, url, token);
}

OpenPolicyAgentAccess::OpenPolicyAgentAccess(const String & role_, const String & url_, const String & service_token_)
    : role(role_)
    , url(url_)
    , service_token(service_token_)
{
}

OpenPolicyAgentAccess::OpenPolicyAgentAccess(const String & role_, const String & url_, const std::pair<String, String> & basic_auth_)
    : role(role_)
    , url(url_)
    , basic_auth(basic_auth_)
{
}

OpenPolicyAgentAccess::~OpenPolicyAgentAccess() = default;


AccessRightsElement OpenPolicyAgentAccess::normalizeRightsElement(const AccessRightsElement & el, const String & current_database)
{
    if (el.wildcard)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OPA: Wildcard access is not supported, needed grant {}", el.toStringWithoutOptions());

    if (el.grant_option)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "OPA: Grant option access is not supported, needed grant {}", el.toStringWithoutOptions());

    AccessRightsElement element = el.isGlobalWithParameter()
        ? (el.anyParameter() ? AccessRightsElement(el.access_flags) : AccessRightsElement(el.access_flags, el.parameter))
        : el;
    element.replaceEmptyDatabase(current_database);
    return element;
}

void OpenPolicyAgentAccess::checkAccess(ContextPtr context, const AccessRightsElement & el) const
{
    const AccessRightsElement element = normalizeRightsElement(el, context->getCurrentDatabase());

    const String data = makeAccessJSON(el, context->getUser());

    HTTPAuthClient<OPAResponseParser> client(HTTPAuthClientParams::createDefault(url, max_tries));

    String poco_error;
    try
    {
        bool granted = false;
        if (!service_token.empty())
            granted = client.authenticateBearer(service_token, {}, data);
        else if (basic_auth)
            granted = client.authenticateBasic(basic_auth->first, basic_auth->second, {}, data);

        context->addQueryPrivilegesInfo(element.toStringWithoutOptions(), granted);
        if (granted)
            return;
    }
    catch (const Poco::Exception & ex)
    {
        poco_error = String(" (Poco: ") + ex.what() + ")";
    }

    throw Exception(
        ErrorCodes::ACCESS_DENIED,
        "OPA: Not enough privileges. To execute this query, it's necessary to have the grant {}",
        (element.toStringWithoutOptions() + poco_error));
}

void OpenPolicyAgentAccess::checkAccess(ContextPtr context, const AccessRightsElements & elements) const
{
    for (const auto & el : elements)
        checkAccess(context, el);
}

void OpenPolicyAgentAccess::checkAccess(ContextPtr context, const AccessFlags & flags) const
{
    checkAccess(context, AccessRightsElement{flags});
}

void OpenPolicyAgentAccess::checkAccess(ContextPtr context, const AccessFlags & flags, std::string_view database) const
{
    checkAccess(context, AccessRightsElement{flags, database});
}

void OpenPolicyAgentAccess::checkAccess(
    ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table) const
{
    checkAccess(context, AccessRightsElement{flags, database, table});
}

void OpenPolicyAgentAccess::checkAccess(
    ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const
{
    checkAccess(context, AccessRightsElement{flags, database, table, column});
}

void OpenPolicyAgentAccess::checkAccess(
    ContextPtr context,
    const AccessFlags & flags,
    std::string_view database,
    std::string_view table,
    const std::vector<std::string_view> & columns) const
{
    checkAccess(context, AccessRightsElement{flags, database, table, columns});
}

void OpenPolicyAgentAccess::checkAccess(
    ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const
{
    checkAccess(context, AccessRightsElement{flags, database, table, columns});
}


}
