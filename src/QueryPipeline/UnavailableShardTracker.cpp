#include <QueryPipeline/UnavailableShardTracker.h>

#include <Interpreters/StorageID.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int TOO_MANY_UNAVAILABLE_SHARDS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
}

bool isTableMissingOnShard(const Exception & e, const StorageID & table, const String & default_database)
{
    if (e.code() != ErrorCodes::UNKNOWN_TABLE && e.code() != ErrorCodes::UNKNOWN_DATABASE)
        return false;

    /// Which names are quoted depends on the version of the shard.
    auto maybe_quoted = [](const String & name)
    {
        String quoted = RE2::QuoteMeta(backQuote(name));
        return isValidIdentifier(name) ? fmt::format("(?:{}|{})", name, quoted) : quoted;
    };

    const String & database = table.hasDatabase() ? table.database_name : default_database;
    const String database_pattern = database.empty() ? R"((?:\w+|`(?:[^`\\]|\\.)*`))" : maybe_quoted(database);
    const String table_pattern = fmt::format(R"({}\.{}(?: \([0-9a-f-]+\))?)", database_pattern, maybe_quoted(table.table_name));

    String pattern = fmt::format(R"(Table {0} does not exist[.:]|Table '{0}' doesn't exists\.)", table_pattern);

    if (!database.empty())
        pattern += fmt::format(R"(|Database {} does not exist[.:])", database_pattern);

    /// These messages have the names without quotes, a name with a dot could be confused with another one.
    if (!database.contains('.') && !table.table_name.contains('.'))
    {
        pattern += fmt::format(
            R"(|Unknown table expression identifier '{}'(?: in scope |\. In scope |\. Maybe you meant\s))",
            RE2::QuoteMeta(table.getQualifiedName().getFullName()));
        pattern += fmt::format(R"(|Unknown table {}\.(?:$| Stack trace:))", RE2::QuoteMeta(database + "." + table.table_name));
    }

    /// Only the beginning of the message: what follows can be the text of the query or an exception from yet another server.
    const RE2 regexp(fmt::format(R"(^Received from \S+\. DB::Exception: (?:{}))", pattern), RE2::Latin1);
    return RE2::PartialMatch(e.message(), regexp);
}

void UnavailableShardTracker::registerExtraUnits(size_t extra)
{
    total_units += extra;
}

void UnavailableShardTracker::seal()
{
    sealed = true;
    throwIfAllUnitsSkipped(unpack(skip_counts.load()).no_data);
}

void UnavailableShardTracker::throwIfAllUnitsSkipped(size_t observed_no_data) const
{
    if (!sealed)
        return;

    const size_t units = total_units;
    if (units > 0 && observed_no_data >= units)
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "No available shards to query");
}

void UnavailableShardTracker::onShardSkipped(bool produced_data)
{
    /// One fetch_add advances both counts, so the pair read back is a state the tracker really held:
    /// no sibling's skip lands inside it, and which error is thrown cannot depend on interleaving.
    const UInt64 delta = (1ULL << 32) | (produced_data ? 0ULL : 1ULL);
    const auto [count, no_data] = unpack(skip_counts.fetch_add(delta) + delta);

    if (max_unavailable_num > 0 && count > max_unavailable_num)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable, "
            "max_skip_unavailable_shards_num is set to {}",
            count, total_shards, max_unavailable_num);

    if (max_unavailable_ratio > 0 && total_shards > 0
        && static_cast<Float64>(count) / static_cast<Float64>(total_shards) > max_unavailable_ratio)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable ({:.1f}%), "
            "max_skip_unavailable_shards_ratio is set to {}",
            count, total_shards, 100.0 * static_cast<double>(count) / static_cast<double>(total_shards), max_unavailable_ratio);

    throwIfAllUnitsSkipped(no_data);
}

}
