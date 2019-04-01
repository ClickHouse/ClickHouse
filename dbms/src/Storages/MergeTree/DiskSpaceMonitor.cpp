#include <Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

std::map<String, DiskSpaceMonitor::DiskReserve> DiskSpaceMonitor::reserved;
std::mutex DiskSpaceMonitor::mutex;

}
