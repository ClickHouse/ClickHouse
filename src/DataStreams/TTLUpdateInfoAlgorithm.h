#pragma once

#include <DataStreams/ITTLAlgorithm.h>

namespace DB
{

/// Calculates new ttl_info and does nothing with data.
class TTLUpdateInfoAlgorithm : public ITTLAlgorithm
{
public:
    TTLUpdateInfoAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override = 0;
};

class TTLMoveAlgorithm final : public TTLUpdateInfoAlgorithm
{
public:
    TTLMoveAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);
    void finalize(const MutableDataPartPtr & data_part) const override;
};

class TTLRecompressionAlgorithm final : public TTLUpdateInfoAlgorithm
{
public:
    TTLRecompressionAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);
    void finalize(const MutableDataPartPtr & data_part) const override;
};

}
