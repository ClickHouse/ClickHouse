#include <DB/Common/ClickHouseRevision.h>
#include <DB/Common/config_version.h>

namespace ClickHouseRevision
{
	unsigned get() { return VERSION_REVISION; }
}
