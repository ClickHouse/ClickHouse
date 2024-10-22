#include <Access/ContextAccessParams.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsBool allow_introspection_functions;
    extern const SettingsUInt64 readonly;
}

ContextAccessParams::ContextAccessParams(
    std::optional<UUID> user_id_,
    bool full_access_,
    bool use_default_roles_,
    const std::shared_ptr<const std::vector<UUID>> & current_roles_,
    const Settings & settings_,
    const String & current_database_,
    const ClientInfo & client_info_)
    : user_id(user_id_)
    , full_access(full_access_)
    , use_default_roles(use_default_roles_)
    , current_roles(current_roles_)
    , readonly(settings_[Setting::readonly])
    , allow_ddl(settings_[Setting::allow_ddl])
    , allow_introspection(settings_[Setting::allow_introspection_functions])
    , current_database(current_database_)
    , interface(client_info_.interface)
    , http_method(client_info_.http_method)
    , address(client_info_.current_address.host())
    , forwarded_address(client_info_.getLastForwardedFor())
    , quota_key(client_info_.quota_key)
    , initial_user((client_info_.initial_user != client_info_.current_user) ? client_info_.initial_user : "")
{
}

String ContextAccessParams::toString() const
{
    WriteBufferFromOwnString out;
    auto separator = [&] { return out.stringView().empty() ? "" : ", "; };
    if (user_id)
        out << separator() << "user_id = " << *user_id;
    if (full_access)
        out << separator() << "full_access = " << full_access;
    if (use_default_roles)
        out << separator() << "use_default_roles = " << use_default_roles;
    if (current_roles && !current_roles->empty())
    {
        out << separator() << "current_roles = [";
        for (size_t i = 0; i != current_roles->size(); ++i)
        {
            if (i)
                out << ", ";
            out << (*current_roles)[i];
        }
        out << "]";
    }
    if (readonly)
        out << separator() << "readonly = " << readonly;
    if (allow_ddl)
        out << separator() << "allow_ddl = " << allow_ddl;
    if (allow_introspection)
        out << separator() << "allow_introspection = " << allow_introspection;
    if (!current_database.empty())
        out << separator() << "current_database = " << current_database;
    out << separator() << "interface = " << magic_enum::enum_name(interface);
    if (http_method != ClientInfo::HTTPMethod::UNKNOWN)
        out << separator() << "http_method = " << magic_enum::enum_name(http_method);
    if (!address.isWildcard())
        out << separator() << "address = " << address.toString();
    if (!forwarded_address.empty())
        out << separator() << "forwarded_address = " << forwarded_address;
    if (!quota_key.empty())
        out << separator() << "quota_key = " << quota_key;
    if (!initial_user.empty())
        out << separator() << "initial_user = " << initial_user;
    return out.str();
}

bool operator ==(const ContextAccessParams & left, const ContextAccessParams & right)
{
    auto check_equals = [](const auto & x, const auto & y)
    {
        if constexpr (::detail::is_shared_ptr_v<std::remove_cvref_t<decltype(x)>>)
        {
            if (!x)
                return !y;
            else if (!y)
                return false;
            else
                return *x == *y;
        }
        else
        {
            return x == y;
        }
    };

    #define CONTEXT_ACCESS_PARAMS_EQUALS(name) \
        if (!check_equals(left.name, right.name)) \
            return false;

    CONTEXT_ACCESS_PARAMS_EQUALS(user_id)
    CONTEXT_ACCESS_PARAMS_EQUALS(full_access)
    CONTEXT_ACCESS_PARAMS_EQUALS(use_default_roles)
    CONTEXT_ACCESS_PARAMS_EQUALS(current_roles)
    CONTEXT_ACCESS_PARAMS_EQUALS(readonly)
    CONTEXT_ACCESS_PARAMS_EQUALS(allow_ddl)
    CONTEXT_ACCESS_PARAMS_EQUALS(allow_introspection)
    CONTEXT_ACCESS_PARAMS_EQUALS(current_database)
    CONTEXT_ACCESS_PARAMS_EQUALS(interface)
    CONTEXT_ACCESS_PARAMS_EQUALS(http_method)
    CONTEXT_ACCESS_PARAMS_EQUALS(address)
    CONTEXT_ACCESS_PARAMS_EQUALS(forwarded_address)
    CONTEXT_ACCESS_PARAMS_EQUALS(quota_key)
    CONTEXT_ACCESS_PARAMS_EQUALS(initial_user)

    #undef CONTEXT_ACCESS_PARAMS_EQUALS

    return true; /// All fields are equal, operator == must return true.
}

bool operator <(const ContextAccessParams & left, const ContextAccessParams & right)
{
    auto check_less = [](const auto & x, const auto & y)
    {
        if constexpr (::detail::is_shared_ptr_v<std::remove_cvref_t<decltype(x)>>)
        {
            if (!x)
                return y ? -1 : 0;
            else if (!y)
                return 1;
            else if (*x == *y)
                return 0;
            else if (*x < *y)
                return -1;
            else
                return 1;
        }
        else
        {
            if (x == y)
                return 0;
            else if (x < y)
                return -1;
            else
                return 1;
        }
    };

    #define CONTEXT_ACCESS_PARAMS_LESS(name) \
        if (auto cmp = check_less(left.name, right.name); cmp != 0) \
            return cmp < 0;

    CONTEXT_ACCESS_PARAMS_LESS(user_id)
    CONTEXT_ACCESS_PARAMS_LESS(full_access)
    CONTEXT_ACCESS_PARAMS_LESS(use_default_roles)
    CONTEXT_ACCESS_PARAMS_LESS(current_roles)
    CONTEXT_ACCESS_PARAMS_LESS(readonly)
    CONTEXT_ACCESS_PARAMS_LESS(allow_ddl)
    CONTEXT_ACCESS_PARAMS_LESS(allow_introspection)
    CONTEXT_ACCESS_PARAMS_LESS(current_database)
    CONTEXT_ACCESS_PARAMS_LESS(interface)
    CONTEXT_ACCESS_PARAMS_LESS(http_method)
    CONTEXT_ACCESS_PARAMS_LESS(address)
    CONTEXT_ACCESS_PARAMS_LESS(forwarded_address)
    CONTEXT_ACCESS_PARAMS_LESS(quota_key)
    CONTEXT_ACCESS_PARAMS_LESS(initial_user)

    #undef CONTEXT_ACCESS_PARAMS_LESS

    return false; /// All fields are equal, operator < must return false.
}

bool ContextAccessParams::dependsOnSettingName(std::string_view setting_name)
{
    return (setting_name == "readonly") || (setting_name == "allow_ddl") || (setting_name == "allow_introspection_functions");
}

}
