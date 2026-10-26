#pragma once


namespace DB
{

struct Settings;

/// Adjusts the copy of the initiator's settings that an interserver sender is about to serialize into a
/// secondary query: `MultiplexedConnections` and `HedgedConnections` for `SELECT`, `RemoteInserter` for
/// distributed `INSERT`. `Connection::sendQuery` serializes only the settings marked as changed, so both
/// adjustments below are about that flag rather than about the values.
///
/// - Values derived from `compatibility` are demoted to unchanged. They still select this side's network
///   codec (`Connection::sendQuery` reads them by value), but the shard re-derives them from the
///   `compatibility` setting itself, which stays changed and is serialized. Sending them as explicit
///   changes instead makes the shard subject them to its own settings constraints - a `CONST` pin drops
///   them silently, a range pin clamps them - so the shard would diverge from the `compatibility` the
///   initiator asked for. This mirrors `ClientBase::settingsWithoutCompatibilityDerived` for the initial
///   query.
/// - `dialect` is forced to ClickHouse SQL and, unlike the demotion, must stay changed so that it really
///   is serialized: the query text the sender ships has already been rewritten into ClickHouse SQL, while
///   the shard otherwise takes the parser from the effective `dialect` of the authenticated user, which
///   its own profile may default to Kusto or PRQL. Sending a value the user already has is a no-op for
///   settings constraints, so this does not trip a profile that pins `dialect` as read-only.
///
/// The demotion runs first, so the `dialect` override is marked changed afterwards.
void prepareSecondaryQuerySettings(Settings & settings);

}
