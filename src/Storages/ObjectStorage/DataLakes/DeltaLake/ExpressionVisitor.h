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

/// Result of a parsed delta-kernel expression.
class ParsedExpression
{
public:
    explicit ParsedExpression(std::shared_ptr<DB::ActionsDAG> dag_, const DB::NamesAndTypesList & schema_);

    /// Get values for the `columns` considering that
    /// they contain literal (constant) values.
    /// This is used, for example, to get partition values.
    std::vector<DB::Field> getConstValues(const DB::Names & columns) const;

    /// Apply parsed expression to the chunk.
    /// `columns` is the "requested columns" -
    /// a subset of table schema columns, which the query needs,
    /// e.g. we might not need to apply the while parsed expression,
    /// but only a part of it.
    void apply(DB::Chunk & chunk, const DB::NamesAndTypesList & chunk_schema, const DB::Names & columns);

    const std::shared_ptr<DB::ActionsDAG> dag;
private:
    const DB::NamesAndTypesList schema;
};

std::unique_ptr<ParsedExpression> visitExpression(
    const ffi::Expression * expression,
    const DB::NamesAndTypesList & expression_schema);

/// A method used in unit test.
std::shared_ptr<DB::ActionsDAG> visitExpression(
    ffi::SharedExpression * expression,
    const DB::NamesAndTypesList & expression_schema);

}

#endif
