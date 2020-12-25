#pragma once

#include <Storages/TTLDescription.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <common/DateLUT.h>

namespace DB
{

class ITTLAlgorithm
{
public:
    using TTLInfo = IMergeTreeDataPart::TTLInfo;
    using MutableDataPartPtr = MergeTreeMutableDataPartPtr;

    ITTLAlgorithm(const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);
    virtual ~ITTLAlgorithm() = default;

    virtual void execute(Block & block) = 0;
    virtual void finalize(const MutableDataPartPtr & data_part) const = 0;

    bool isMinTTLExpired() const { return force || isTTLExpired(old_ttl_info.min); }
    bool isMaxTTLExpired() const { return isTTLExpired(old_ttl_info.max); }

protected:
    bool isTTLExpired(time_t ttl) const;
    UInt32 getTimestampByIndex(const IColumn * column, size_t index) const;
    static ColumnPtr extractRequieredColumn(const ExpressionActionsPtr & expression, const Block & block, const String & result_column);

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
