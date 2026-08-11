#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `system.hybrid_watermarks` table, which exposes the current
  * effective watermark values for every attached Hybrid-engine table.
  *
  * For each live Hybrid table, emits exactly one of:
  *   - N rows, one per declared `hybridParam()` name;
  *   - 0 rows, if the table has no `hybridParam()` references;
  *   - 1 diagnostic row with `last_exception` populated, on read failure or
  *     post-read consistency violation.
  *
  * Non-Hybrid tables are filtered out. Tables that fail to load from metadata
  * at startup never attach and therefore never appear here — they surface in
  * server logs instead.
  */
class StorageSystemHybridWatermarks final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemHybridWatermarks"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
