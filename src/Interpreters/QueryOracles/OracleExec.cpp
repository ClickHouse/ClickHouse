#include <Interpreters/QueryOracles/OracleExec.h>
#include <Interpreters/QueryOracles/OracleSettings.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
extern const int TOO_MANY_BYTES;
}

namespace
{

/// Split TabSeparated output into rows. ClickHouse always terminates a TSV row with `\n`, so the
/// canonical form is `<row1>\n...\n<rowN>\n`. Strip exactly one trailing `\n` if present (it
/// terminates the last row, not a separator), then split on `\n`. This produces the right number
/// of rows even when many are empty strings (e.g. unmatched LEFT JOIN rows for a String column).
Rows splitIntoRows(const std::string & output)
{
    Rows rows;
    if (output.empty())
        return rows;

    std::string_view sv{output};
    if (sv.back() == '\n')
        sv.remove_suffix(1);

    size_t start = 0;
    while (true)
    {
        size_t end = sv.find('\n', start);
        if (end == std::string_view::npos)
        {
            rows.emplace_back(sv.substr(start));
            break;
        }
        rows.emplace_back(sv.substr(start, end - start));
        start = end + 1;
    }

    return rows;
}

void shapeRows(Rows & rows, ResultShape shape)
{
    if (shape == ResultShape::Ordered)
        return;
    std::sort(rows.begin(), rows.end());
    if (shape == ResultShape::SortedSet)
        rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
}

}

ContextMutablePtr OracleExec::makeOracleContext(const ContextMutablePtr & base_context)
{
    auto session_context = Context::createCopy(base_context);
    session_context->makeSessionContext();

    auto oracle_context = Context::createCopy(session_context);
    oracle_context->makeQueryContext();

    /// Apply every neutralization from the single source of truth (QueryOracles/OracleSettings).
    /// This is the ONLY place these pins are applied; `isPinnedByOracleContext` mirrors the same
    /// list so the setting-flip sweep can never flip a pinned setting. Rationale for each pin
    /// lives in the `why` field there (single-thread pin closes a pipeline-teardown UAF; the
    /// read/result caps throw rather than truncate; `readonly`/`implicit_transaction` off keep
    /// fixture DDL executable).
    for (const auto & pin : oraclePinnedSettings())
        oracle_context->setSetting(String(pin.name), pin.value);

    oracle_context->setCurrentQueryId("");
    return oracle_context;
}

std::optional<Rows> OracleExec::executeRows(
    const std::string & sql, const ContextMutablePtr & base_context,
    ResultShape shape, const SettingsOverlay & overlay)
{
    auto oracle_context = makeOracleContext(base_context);
    oracle_context->setDefaultFormat("TabSeparated");
    for (const auto & [name, value] : overlay)
        oracle_context->setSetting(name, value);

    /// Use the ReadBuffer/WriteBuffer executeQuery API — crash-safe because ClickHouse handles all
    /// column serialization within the pipeline internally, writing formatted text to the buffer.
    ReadBufferFromString istr(sql);
    WriteBufferFromOwnString ostr;

    try
    {
        executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});
    }
    catch (const Exception & e)
    {
        /// `result_overflow_mode=throw` makes oracle sub-queries that exceed max_result_rows /
        /// max_result_bytes throw rather than silently truncate. Signal "skipped" so the oracle
        /// never compares partial results.
        if (e.code() == ErrorCodes::TOO_MANY_ROWS || e.code() == ErrorCodes::TOO_MANY_BYTES)
            return std::nullopt;
        throw;
    }

    std::string output = ostr.str();
    if (output.size() > MAX_ORACLE_OUTPUT_SIZE)
        return std::nullopt; /// Belt-and-braces: still cap the formatted output.

    Rows rows = splitIntoRows(output);
    shapeRows(rows, shape);
    return rows;
}

OracleExec::ExecOutcome OracleExec::tryExecuteRows(
    const std::string & sql, const ContextMutablePtr & base_context,
    ResultShape shape, const SettingsOverlay & overlay)
{
    ExecOutcome outcome;
    try
    {
        auto rows = executeRows(sql, base_context, shape, overlay);
        if (rows)
            outcome.rows = std::move(rows);
        else
            outcome.overflow = true;
    }
    catch (const Exception & e)
    {
        outcome.error_code = e.code();
        outcome.error_message = e.message();
    }
    catch (...)
    {
        outcome.error_code = -1;
        outcome.error_message = getCurrentExceptionMessage(false);
    }
    return outcome;
}

std::optional<Field> OracleExec::executeScalar(
    const std::string & sql, const ContextMutablePtr & base_context,
    const SettingsOverlay & overlay)
{
    auto oracle_context = makeOracleContext(base_context);
    for (const auto & [name, value] : overlay)
        oracle_context->setSetting(name, value);

    auto result = executeQuery(sql, oracle_context, QueryFlags{.internal = true});

    if (!result.second.pipeline.initialized() || !result.second.pipeline.pulling())
        return std::nullopt;

    PullingPipelineExecutor executor(result.second.pipeline);
    Block block;

    while (executor.pull(block))
    {
        if (block.rows() > 0 && block.columns() > 0)
        {
            Field scalar;
            block.getByPosition(0).column->get(0, scalar);
            /// Drain the remaining blocks so the pipeline finalizes cleanly.
            while (executor.pull(block)) {}
            return scalar;
        }
    }

    return std::nullopt;
}

bool OracleExec::executeStatement(
    const std::string & sql, const ContextMutablePtr & base_context,
    const SettingsOverlay & overlay)
{
    try
    {
        auto oracle_context = makeOracleContext(base_context);
        for (const auto & [name, value] : overlay)
            oracle_context->setSetting(name, value);

        ReadBufferFromString istr(sql);
        WriteBufferFromOwnString ostr;
        executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});
        return true;
    }
    catch (...)
    {
        /// Fail-close: a fixture/DDL statement that cannot run means the oracle must not proceed.
        return false;
    }
}

bool OracleExec::isStable(
    const std::string & reference_sql, const Rows & previous,
    const ContextMutablePtr & base_context, ResultShape shape,
    const SettingsOverlay & overlay)
{
    auto again = executeRows(reference_sql, base_context, shape, overlay);
    /// Fail-close: an overflow or missing re-read is not a proven-stable read.
    return again.has_value() && *again == previous;
}

}
