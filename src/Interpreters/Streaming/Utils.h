#pragma once

#include <Core/Names.h>
#include <Core/Streaming/Settings.h>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

class ActionsDAG;
class Block;
class NamesAndTypesList;

/// Whether a partition stayed inactive longer than the watermark idle timeout (never expires when the timeout is unset).
bool isIdleExpired(
    const std::chrono::steady_clock::time_point & now,
    const std::chrono::steady_clock::time_point & last_active,
    const WatermarkSettingsPtr & watermark);

/// Resolves the watermark expression against the header columns and builds an executable DAG for it.
ActionsDAG buildWatermarkActionsDAG(
    const ASTPtr & watermark_expression,
    const Block & header,
    const ContextPtr & context);

/// Source columns the watermark expression depends on, derived from the analyzed expression (a raw AST walk would treat lambda-local names as storage columns).
Names collectWatermarkSourceColumns(
    const ASTPtr & watermark_expression,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context);

}
