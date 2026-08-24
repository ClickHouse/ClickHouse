#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>

namespace ClickHouseRevision
{
    unsigned getVersionRevision() { return VERSION_REVISION; }
    unsigned getVersionInteger() { return VERSION_INTEGER; }
}
