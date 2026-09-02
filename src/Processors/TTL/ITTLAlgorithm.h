#pragma once

#include <Storages/TTLDescription.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/DateLUT.h>

namespace DB
{

struct TTLExpressions
{
    ExpressionActionsPtr expression;
    ExpressionActionsPtr where_expression;
};

/**
 * Represents the actions, which are required to do
 * with data, when TTL is expired: delete, aggregate, etc.
 */
class ITTLAlgorithm
{
public:
    using TTLInfo = IMergeTreeDataPart::TTLInfo;
    using MutableDataPartPtr = MergeTreeMutableDataPartPtr;

    ITTLAlgorithm(const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);
    virtual ~ITTLAlgorithm() = default;

    virtual void execute(Block & block) = 0;

    /// Updates TTL metadata of the data_part.
    virtual void finalize(const MutableDataPartPtr & data_part) const = 0;

    bool isMinTTLExpired() const { return force || isTTLExpired(old_ttl_info.min); }
    bool isMaxTTLExpired() const { return isTTLExpired(old_ttl_info.max); }

    /// Resolve the column type once and fill `timestamps` for the whole block. Every TTL algorithm
    /// and `TTLDeleteFilterTransform` map a TTL result column to Unix timestamps through here.
    /// The insert path does not: `MergeTreeDataWriter::updateTTLInfo` / `updateTTLInfoConst` dispatch
    /// over the same types separately, so a new TTL result type has to be added there as well.
    static void extractTimestamps(
        const IColumn * column, const DateLUTImpl & date_lut, PaddedPODArray<Int64> & timestamps);

    /** This function is needed to avoid a conflict between already calculated columns and columns that needed to execute TTL.
      * If result column is absent in block, all required columns are copied to new block and expression is executed on new block.
      */
    static ColumnPtr executeExpressionAndGetColumn(
        const ExpressionActionsPtr & expression, const Block & block, const String & result_column);

protected:
    bool isTTLExpired(time_t ttl) const;

    /// Fill `timestamps` from `column` using this algorithm's time zone. Call it once per block,
    /// then index `timestamps` in the row loop. The buffer stays owned by the caller: the transforms
    /// keep one algorithm object per TTL rule alive for the whole merge, and `execute` runs them one
    /// after another, so a member would pin one array per rule for no benefit.
    void extractTimestamps(const IColumn * column, PaddedPODArray<Int64> & timestamps) const
    {
        extractTimestamps(column, date_lut, timestamps);
    }

    const TTLExpressions ttl_expressions;
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
