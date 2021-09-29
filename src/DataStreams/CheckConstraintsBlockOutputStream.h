#pragma once

#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/** Check for constraints violation. If anything is found - throw an exception with detailed error message.
  * Otherwise just pass block to output unchanged.
  */

class CheckConstraintsTransform final : public ExceptionKeepingTransform
{
public:
    CheckConstraintsTransform(
            const StorageID & table_,
            const Block & header,
            const ConstraintsDescription & constraints_,
            ContextPtr context_);

    String getName() const override { return "CheckConstraintsTransform"; }

    void transform(Chunk & chunk) override;

private:
    StorageID table_id;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
    size_t rows_written = 0;
};
}
