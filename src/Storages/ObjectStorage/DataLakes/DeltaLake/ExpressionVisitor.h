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

/// Get values for the `columns` considering that
/// they contain literal (constant) values.
/// This is used, for example, to get partition values.
std::vector<DB::Field> getConstValuesFromExpression(
    const DB::Names & columns,
    const DB::ActionsDAG & dag);

/// Visit exception for scanCallback.
std::shared_ptr<DB::ActionsDAG> visitScanCallbackExpression(
    const ffi::Expression * expression,
    const DB::NamesAndTypesList & expression_schema);

/// A method used in unit test.
std::shared_ptr<DB::ActionsDAG> visitExpression(
    ffi::SharedExpression * expression,
    const DB::NamesAndTypesList & expression_schema);

}

#endif
