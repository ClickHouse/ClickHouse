#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace DB
{

class ASTSelectQuery;

/// Per-oracle relaxations of the standard `isSafeForOracle` shape gate. Each bit skips exactly
/// one rejection clause; an oracle that sets a bit takes ownership of the soundness obligation
/// that clause protected. `Bespoke` is a marker (not a relaxation): the oracle does not use the
/// standard gate at all and enumerates its own predicates in `isApplicable`.
enum class GateRelax : uint32_t
{
    None                   = 0,
    AllowArrayJoinClause   = 1u << 0,
    AllowGroupingModifiers = 1u << 1,   /// CUBE / ROLLUP / GROUPING SETS / GROUP BY ALL
    AllowDistinct          = 1u << 2,
    AllowLimit             = 1u << 3,   /// LIMIT / OFFSET / LIMIT BY / WITH TIES
    AllowPrewhere          = 1u << 4,
    AllowWindow            = 1u << 5,   /// with the oracle's own window allowlist on top

    /// The setting-flip sweep's looser gate: both sides run byte-identical SQL, so shape
    /// restrictions that exist only to protect structural rewrites do not apply.
    IdenticalTextBothSides = AllowGroupingModifiers | AllowDistinct | AllowLimit | AllowPrewhere,

    /// Marker: this oracle enumerates a bespoke predicate list in `isApplicable`.
    Bespoke                = 1u << 31,
};

inline GateRelax operator|(GateRelax a, GateRelax b)
{
    return static_cast<GateRelax>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}
inline bool hasRelax(GateRelax mask, GateRelax bit)
{
    return (static_cast<uint32_t>(mask) & static_cast<uint32_t>(bit)) != 0;
}

/// Result of resolving whether any table in a query matches a predicate. Catalog-resolution
/// failure is explicit (`Unresolvable`) so the caller decides its safety direction (see
/// `OracleGate.cpp` for the uniform "failure steers toward skip" rule).
enum class ResolveMatch : uint8_t { Yes, No, Unresolvable };

/// Exactly one physical table in FROM (no JOIN / ARRAY JOIN / subquery / table function),
/// resolved through DatabaseCatalog. nullptr on any failure or ambiguity.
StoragePtr resolveSingleTableStorage(const ASTSelectQuery & select, const ContextPtr & context);

/// Walk every ASTTableIdentifier in `ast`, resolve each through DatabaseCatalog, and apply
/// `predicate`. `Yes` if any resolved storage matches; `Unresolvable` if a table could not be
/// resolved (and none matched); otherwise `No`.
ResolveMatch referencesTableMatching(
    const ASTPtr & ast, const ContextPtr & context,
    const std::function<bool(const StoragePtr &)> & predicate);

/// True iff, grouped by `key_exprs` over `inner_sql`, every group renders exactly one distinct
/// projected tuple — i.e. the key is a total order for positional/representative comparison.
/// Implemented as one scalar sub-query; returns false on any execution failure (fail-close).
bool hasTotalOrderKey(
    const std::string & inner_sql,
    const std::vector<std::string> & key_exprs,
    const std::vector<std::string> & projection_exprs,
    const ContextMutablePtr & context);

}
