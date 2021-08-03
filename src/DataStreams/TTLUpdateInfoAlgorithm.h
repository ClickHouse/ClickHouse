#pragma once

#include <DataStreams/ITTLAlgorithm.h>

namespace DB
{

enum class TTLUpdateType
{
    COLUMNS_TTL,
    TABLE_TTL,
    ROWS_WHERE_TTL,
    MOVES_TTL,
    RECOMPRESSION_TTL,
    GROUP_BY_TTL,
};

/// Calculates new ttl_info and does nothing with data.
class TTLUpdateInfoAlgorithm : public ITTLAlgorithm
{
public:
    TTLUpdateInfoAlgorithm(
        const TTLDescription & description_,
        const TTLUpdateType ttl_update_type_,
        const String ttl_update_key_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_, bool force_
    );

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    const TTLUpdateType ttl_update_type;
    const String ttl_update_key;
};


}
