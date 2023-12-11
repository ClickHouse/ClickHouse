#include <Processors/TTL/TTLDropAlgorithm.h>

namespace DB
{

TTLDropAlgorithm::TTLDropAlgorithm(
    const DB::TTLDescription & description_,
    const DB::ITTLAlgorithm::TTLInfo & old_ttl_info_,
    time_t current_time_, bool force_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
    if (!isMinTTLExpired())
        new_ttl_info = old_ttl_info;
}

void TTLDropAlgorithm::execute(Block & block)
{
    if (!block || !isMaxTTLExpired())
        return;

    new_ttl_info.ttl_finished = true;

    block.clear();
}

void TTLDropAlgorithm::finalize(const ITTLAlgorithm::MutableDataPartPtr & data_part) const
{
    data_part->ttl_infos.table_ttl = new_ttl_info;
}

}
