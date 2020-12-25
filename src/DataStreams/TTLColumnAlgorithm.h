#pragma once

#include <DataStreams/ITTLAlgorithm.h>

namespace DB
{

class TTLColumnAlgorithm final : public ITTLAlgorithm
{
public:
    TTLColumnAlgorithm(
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_,
        bool force_,
        const String & column_name_,
        const ExpressionActionsPtr & default_expression_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    const String column_name;
    const ExpressionActionsPtr default_expression;

    bool is_fully_empty = true;
};

}
