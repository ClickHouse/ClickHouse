#pragma once

#include <base/types.h>
#include <unordered_set>

namespace DB
{

class ServerType
{
public:
    enum Type
    {
        TCP_WITH_PROXY,
        TCP_SECURE,
        TCP,
        HTTP,
        HTTPS,
        MYSQL,
        GRPC,
        POSTGRESQL,
        PROMETHEUS,
        CUSTOM,
        INTERSERVER_HTTP,
        INTERSERVER_HTTPS,
        QUERIES_ALL,
        QUERIES_DEFAULT,
        QUERIES_CUSTOM,
        CLOUD,
        END
    };

    using Types = std::unordered_set<Type>;
    using CustomNames = std::unordered_set<String>;

    ServerType() = default;

    explicit ServerType(
        Type type_,
        const std::string & custom_name_ = "",
        const Types & exclude_types_ = {},
        const CustomNames exclude_custom_names_ = {})
        : type(type_),
          custom_name(custom_name_),
          exclude_types(exclude_types_),
          exclude_custom_names(exclude_custom_names_) {}

    static const char * serverTypeToString(Type type);

    /// Checks whether provided in the arguments type should be started or stopped based on current server type.
    bool shouldStart(Type server_type, const std::string & server_custom_name = "") const;
    bool shouldStop(const std::string & port_name) const;

    Type type;
    std::string custom_name;

    Types exclude_types;
    CustomNames exclude_custom_names;
};

}
