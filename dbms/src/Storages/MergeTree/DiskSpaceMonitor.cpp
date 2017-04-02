#include <Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

size_t DiskSpaceMonitor::reserved_bytes;
size_t DiskSpaceMonitor::reservation_count;
std::mutex DiskSpaceMonitor::mutex;

}
