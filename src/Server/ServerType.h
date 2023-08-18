#pragma once

#include <base/types.h>

namespace DB
{

class ServerType
{
public:
    enum Type
    {
        TCP,
        TCP_WITH_PROXY,
        TCP_SECURE,
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
        END
    };

    ServerType() = default;
    explicit ServerType(Type type_, const std::string & custom_name_ = "") : type(type_), custom_name(custom_name_) {}

    static const char * serverTypeToString(Type type);

    /// Checks whether provided in the arguments type should be started or stopped based on current server type.
    bool shouldStart(Type server_type, const std::string & server_custom_name = "") const;
    bool shouldStop(const std::string & port_name) const;

    Type type;
    std::string custom_name;
};

}
