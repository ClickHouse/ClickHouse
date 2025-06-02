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

namespace DeltaLake
{

class ParsedExpression
{
public:
    using ParsedResult = std::map<size_t, DB::ActionsDAG::NodeRawConstPtrs>;

    ParsedExpression(ParsedResult && result_, DB::ActionsDAG && dag_);

    std::vector<DB::Field> getPartitionValues(const std::vector<size_t> & partition_column_ids);

private:
    DB::ActionsDAG dag;
    ParsedResult result;
};

std::unique_ptr<ParsedExpression> visitExpression(
    const ffi::Expression * expression,
    const DB::NamesAndTypesList & expression_schema);

/// A method used in unit test.
DB::ActionsDAG visitExpression(ffi::SharedExpression * expression, const DB::NamesAndTypesList & expression_schema);

}

#endif
