#pragma once

#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

/// TabSeparated result rows, one string per row (tab-separated columns).
using Rows = std::vector<std::string>;
/// Per-execution setting overrides layered on top of the pinned oracle context.
using SettingsOverlay = std::vector<std::pair<std::string, Field>>;

/// How an oracle wants a result collected for comparison.
enum class ResultShape : uint8_t
{
    SortedBag,   /// sorted, duplicates kept — the default for every equality oracle
    SortedSet,   /// sorted + deduplicated — only with a written per-oracle justification
    Ordered,     /// row order preserved — only under a total-order precheck / determinism argument
};

/// The single execution layer for oracle sub-queries. Wraps `makeOracleContext` (which pins the
/// neutralizing settings from QueryOracles/OracleSettings) and the crash-safe
/// ReadBuffer/WriteBuffer `executeQuery` path, so no oracle re-implements execution or shaping.
class OracleExec
{
public:
    /// Build the pinned, single-threaded query context every oracle sub-query runs under.
    static ContextMutablePtr makeOracleContext(const ContextMutablePtr & base_context);

    /// The standard row-collecting entry point. `std::nullopt` on a result-cap overflow
    /// (TOO_MANY_ROWS / TOO_MANY_BYTES, or output > MAX_ORACLE_OUTPUT_SIZE) — the caller MUST
    /// skip rather than treat it as an empty result. Any other execution error THROWS; the
    /// dispatch loop downgrades it to a skip.
    static std::optional<Rows> executeRows(
        const std::string & sql, const ContextMutablePtr & base_context,
        ResultShape shape, const SettingsOverlay & overlay = {});

    /// Error-observing variant for oracles that must react to a specific error class instead of
    /// skipping. Never throws.
    struct ExecOutcome
    {
        std::optional<Rows> rows;        /// set on success
        bool overflow = false;           /// result-cap overflow — treat exactly like nullopt above
        std::optional<int> error_code;   /// set when execution failed with an exception
        std::string error_message;
    };
    static ExecOutcome tryExecuteRows(
        const std::string & sql, const ContextMutablePtr & base_context,
        ResultShape shape, const SettingsOverlay & overlay = {});

    /// Single-value queries (counts, prechecks, type probes). `std::nullopt` iff the query
    /// produced no value; throws on execution error.
    static std::optional<Field> executeScalar(
        const std::string & sql, const ContextMutablePtr & base_context,
        const SettingsOverlay & overlay = {});

    /// DDL / INSERT / ALTER / OPTIMIZE / SYSTEM statements. Fail-close: returns false on ANY
    /// error, never throws. Output discarded. Seed INSERTs must be `INSERT ... SELECT`.
    static bool executeStatement(
        const std::string & sql, const ContextMutablePtr & base_context,
        const SettingsOverlay & overlay = {});

    /// Re-execution stability guard: re-runs `reference_sql` once with the same shape/overlay and
    /// returns true iff it reproduces `previous`. False => the read is unstable => the oracle
    /// MUST skip. Returns false on overflow/failure (fail-close: an unverifiable read is unstable).
    static bool isStable(
        const std::string & reference_sql, const Rows & previous,
        const ContextMutablePtr & base_context, ResultShape shape,
        const SettingsOverlay & overlay = {});
};

}
