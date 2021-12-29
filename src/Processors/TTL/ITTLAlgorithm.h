#pragma once

#include <Storages/TTLDescription.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/DateLUT.h>

namespace DB
{

/**
 * Represents the actions, which are required to do
 * with data, when TTL is expired: delete, aggregate, etc.
 */
class ITTLAlgorithm
{
public:
    using TTLInfo = IMergeTreeDataPart::TTLInfo;
    using MutableDataPartPtr = MergeTreeMutableDataPartPtr;

    ITTLAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);
    virtual ~ITTLAlgorithm() = default;

    virtual void execute(Block & block) = 0;

    /// Updates TTL metadata of the data_part.
    virtual void finalize(const MutableDataPartPtr & data_part) const = 0;

    bool isMinTTLExpired() const { return force || isTTLExpired(old_ttl_info.min); }
    bool isMaxTTLExpired() const { return isTTLExpired(old_ttl_info.max); }

    /** This function is needed to avoid a conflict between already calculated columns and columns that needed to execute TTL.
      * If result column is absent in block, all required columns are copied to new block and expression is executed on new block.
      */
    static ColumnPtr executeExpressionAndGetColumn(
        const ExpressionActionsPtr & expression, const Block & block, const String & result_column);

protected:
    bool isTTLExpired(time_t ttl) const;
    UInt32 getTimestampByIndex(const IColumn * column, size_t index) const;

    const TTLDescription description;
    const TTLInfo old_ttl_info;
    const time_t current_time;
    const bool force;
    TTLInfo new_ttl_info;

private:
    const DateLUTImpl & date_lut;
};

using TTLAlgorithmPtr = std::unique_ptr<ITTLAlgorithm>;

}
