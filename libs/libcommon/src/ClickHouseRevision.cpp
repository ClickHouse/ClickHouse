#include <common/ClickHouseRevision.h>
#include "common/config_version.h"

namespace ClickHouseRevision
{
	unsigned get() { return VERSION_REVISION; }
}
