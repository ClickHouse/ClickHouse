#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

size_t DiskSpaceMonitor::reserved_bytes;
size_t DiskSpaceMonitor::reservation_count;
Poco::FastMutex DiskSpaceMonitor::mutex;

}
