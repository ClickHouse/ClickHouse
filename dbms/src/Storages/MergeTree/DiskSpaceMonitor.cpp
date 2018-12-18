#include <Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

UInt64 DiskSpaceMonitor::reserved_bytes;
size_t DiskSpaceMonitor::reservation_count;
std::mutex DiskSpaceMonitor::mutex;

}
