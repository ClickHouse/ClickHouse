#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Interpreters/ActionsDAG.h>
#include <Core/Field.h>

namespace ffi
{
struct Expression;
struct SharedExpression;
}
namespace DB
{
class Chunk;
}

namespace DeltaLake
{

class ParsedExpression
{
public:
    explicit ParsedExpression(DB::ActionsDAG && dag_, const DB::NamesAndTypesList & schema_);

    std::vector<DB::Field> getConstValues(const DB::Names & columns) const;

    void apply(DB::Chunk & chunk, const DB::NamesAndTypesList & chunk_schema, const DB::Names & columns);

private:
    const DB::ActionsDAG dag;
    const DB::NamesAndTypesList schema;
};

std::unique_ptr<ParsedExpression> visitExpression(
    const ffi::Expression * expression,
    const DB::NamesAndTypesList & expression_schema);

/// A method used in unit test.
DB::ActionsDAG visitExpression(ffi::SharedExpression * expression, const DB::NamesAndTypesList & expression_schema);

}

#endif
