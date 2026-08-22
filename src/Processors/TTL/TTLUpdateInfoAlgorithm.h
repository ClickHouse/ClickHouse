#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

enum class TTLUpdateField : uint8_t
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
        const TTLExpressions & ttl_expressions_,
        const TTLDescription & description_,
        TTLUpdateField ttl_update_field_,
        String ttl_update_key_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_, bool force_
    );

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    const TTLUpdateField ttl_update_field;
    const String ttl_update_key;
};


}
