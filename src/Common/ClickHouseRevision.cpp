#include "src/Common/ClickHouseRevision.h"

#if !defined(ARCADIA_BUILD)
#    include "src/Common/config_version.h"
#endif

namespace ClickHouseRevision
{
    unsigned get() { return VERSION_REVISION; }
    unsigned getVersionInteger() { return VERSION_INTEGER; }
}
