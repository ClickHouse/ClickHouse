#include <Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

UInt64 DiskSpaceMonitor::reserved_bytes;
UInt64 DiskSpaceMonitor::reservation_count;
std::mutex DiskSpaceMonitor::mutex;

}
