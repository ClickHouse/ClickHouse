#include <Common/PortUtils.h>

#include <limits>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

Int32 getPortOffsetFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    const Int64 offset = config.getInt64("port_offset", 0);
    if (offset < std::numeric_limits<Int32>::min() || offset > std::numeric_limits<Int32>::max())
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Invalid port_offset {}: must be in range {}-{}",
            offset,
            std::numeric_limits<Int32>::min(),
            std::numeric_limits<Int32>::max());
    return static_cast<Int32>(offset);
}

}
