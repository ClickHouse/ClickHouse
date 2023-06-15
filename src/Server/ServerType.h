#pragma once

namespace DB
{

enum class ServerType
{
    UNKNOWN,
    QUERIES,
    CUSTOM,
    TCP,
    TCP_WITH_PROXY,
    TCP_SECURE,
    HTTP,
    HTTPS,
    MYSQL,
    GRPC,
    POSTGRESQL,
    PROMETHEUS,
    END
};

const char * serverTypeToString(ServerType type);

}
