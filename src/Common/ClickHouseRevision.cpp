#include <Common/ClickHouseRevision.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace ClickHouseRevision
{
    unsigned getVersionRevision() { return VERSION_REVISION; }
    unsigned getVersionInteger() { return VERSION_INTEGER; }
}
