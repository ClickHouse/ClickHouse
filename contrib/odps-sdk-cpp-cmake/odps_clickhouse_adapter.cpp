#include <odps_clickhouse_adapter.h>

#include <tunnel/util.h>
#include <odps_table.h>

namespace apsara::odps::sdk::clickhouse
{

std::string resolveTunnelEndpoint(const Configuration & configuration, const std::string & project)
{
    return internal::tunnel::GetRouterServer(configuration, project);
}

const char * getJSONValue(const ODPSTableRecord & record, uint32_t idx, uint32_t & length)
{
    return record.GetJsonValue(idx, length);
}

bool isArrowDeserializeError(std::string_view message)
{
    return message.starts_with("ArrowHttpInputStream Deserialize Exception");
}

}
