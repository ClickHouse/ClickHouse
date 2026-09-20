#include <odps_clickhouse_adapter.h>

#include <tunnel/util.h>

namespace apsara::odps::sdk::clickhouse
{

std::string resolveTunnelEndpoint(const Configuration & configuration, const std::string & project)
{
    return internal::tunnel::GetRouterServer(configuration, project);
}

}
