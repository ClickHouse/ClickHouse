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

/// Marks the settings that choose the method of an aggregation key as changed, with their effective values:
/// `enable_packed_string_keys_in_aggregation` and `enable_fixed_and_packed_string_keys_in_aggregation`.
/// For the senders of `SELECT` secondary queries, `MultiplexedConnections` and `HedgedConnections`.
/// Two-level aggregation bucket numbers depend on the method, so all servers of a distributed query must
/// agree on these values, even when they come only from the initiator's server or profile defaults. Such
/// values are not marked changed, so `Connection::sendQuery` would not send them otherwise.
void markAggregationMethodSettingsChanged(Settings & settings);

/// The receiving side of `markAggregationMethodSettingsChanged`, for the server of a secondary query. Returns
/// whether `settings`, the settings of the query after the server applied its settings constraints to the
/// settings that the initiator passed (`initiator_settings`), differ from them in a setting that chooses the
/// method of an aggregation key. A setting that the initiator did not pass does not count. A constraint drops
/// or clamps a passed value that it does not allow, and the server then numbers two-level buckets differently
/// from the initiator. For example, a remote user whose profile pins
/// `enable_packed_string_keys_in_aggregation = 0` as `READONLY` aggregates a single `String` key with the
/// legacy method, while the initiator takes the packed one, and the two methods put a 12-byte key into
/// different buckets.
bool aggregationMethodSettingsDifferFromInitiator(const Settings & initiator_settings, const Settings & settings);

}
