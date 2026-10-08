#pragma once

#include <Common/ProfileEvents.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTSelectQuery.h>

#include <string_view>

namespace DB
{

class QueryOracleChecker;

/// Immutable description of one correctness oracle: the human-readable name used in
/// log lines and mismatch messages, and the dedicated ProfileEvent counting how often
/// the oracle actually performed a comparison.
struct OracleTraits
{
    std::string_view name;
    ProfileEvents::Event event;
};

/// A single correctness oracle. `QueryOracleChecker::check` dispatches over the ordered
/// `OracleRegistry` instead of a hand-written try/catch ladder, so adding an oracle is one
/// registry line plus one file.
///
/// Phase 0 of the oracle-suite migration wraps the existing `QueryOracleChecker::check*`
/// methods in thin adapters (see `OracleRegistry.cpp`); later oracles get their own
/// `IOracle` subclasses in this directory.
class IOracle
{
public:
    virtual ~IOracle() = default;

    virtual const OracleTraits & traits() const = 0;

    /// Run the oracle on a fuzzed simple SELECT. Returns true iff a comparison was actually
    /// performed (drives the `any_check_performed` result of `check` and the per-oracle
    /// ProfileEvent). A real mismatch is reported by throwing `AST_FUZZER_ORACLE_MISMATCH`;
    /// any other exception is downgraded to a silent skip by the dispatch loop.
    virtual bool run(QueryOracleChecker & checker, const ASTSelectQuery & select, const ContextMutablePtr & context) const = 0;
};

}
