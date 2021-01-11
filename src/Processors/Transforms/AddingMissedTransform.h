#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{


/** This stream adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * All three types of columns are materialized (not constants).
  */
class AddingMissedTransform : public ISimpleTransform
{
public:
    AddingMissedTransform(
        Block header_,
        Block result_header_,
        const ColumnsDescription & columns_,
        const Context & context_);

    String getName() const override { return "AddingMissed"; }

private:
    void transform(Chunk &) override;

    const ColumnsDescription columns;
    const Context & context;
};


}
