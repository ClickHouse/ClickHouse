#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

class TTLDropAlgorithm : public ITTLAlgorithm
{
public:
    TTLDropAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;
};

}
