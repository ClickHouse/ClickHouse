#include <DataStreams/TTLUpdateInfoAlgorithm.h>

namespace DB
{

TTLUpdateInfoAlgorithm::TTLUpdateInfoAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
}

void TTLUpdateInfoAlgorithm::execute(Block & block)
{
    if (!block)
        return;

    auto ttl_column = executeExpressionAndGetColumn(description.expression, block, description.result_column);
    for (size_t i = 0; i < block.rows(); ++i)
    {
        UInt32 cur_ttl = ITTLAlgorithm::getTimestampByIndex(ttl_column.get(), i);
        new_ttl_info.update(cur_ttl);
    }
}

TTLMoveAlgorithm::TTLMoveAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : TTLUpdateInfoAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
}

void TTLMoveAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    data_part->ttl_infos.moves_ttl[description.result_column] = new_ttl_info;
}

TTLRecompressionAlgorithm::TTLRecompressionAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : TTLUpdateInfoAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
}

void TTLRecompressionAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    data_part->ttl_infos.recompression_ttl[description.result_column] = new_ttl_info;
}

}
