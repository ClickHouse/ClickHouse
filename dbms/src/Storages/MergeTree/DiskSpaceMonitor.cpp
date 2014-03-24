#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

size_t DiskSpaceMonitor::reserved_bytes;
Poco::FastMutex DiskSpaceMonitor::reserved_bytes_mutex;

}
