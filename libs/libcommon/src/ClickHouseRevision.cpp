#include <common/ClickHouseRevision.h>
#include "revision.h"

namespace ClickHouseRevision
{
	unsigned get() { return REVISION; }
}
