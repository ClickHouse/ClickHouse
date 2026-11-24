#include <Interpreters/QueryOracles/OracleFixture.h>

#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/String.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>

#include <atomic>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 readonly;
    extern const SettingsBool allow_ddl;
}

namespace
{
/// Process-global sequence for collision-free fixture names (no Date/rand available here).
std::atomic<UInt64> fixture_sequence{0};

bool isIdentifierChar(char c)
{
    return c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

/// True if `name` occurs in `sql` as a whole identifier token, not merely as a substring. Needed
/// because fixture names share a prefix and differ only by a trailing sequence number, so a plain
/// `sql.contains("__oracle_fx_x_1")` would also match `__oracle_fx_x_10` and wrongly arm a DROP of
/// a table this instance never created (e.g. a preserved repro after a restart).
bool sqlNamesIdentifier(const std::string & sql, const std::string & name)
{
    for (size_t pos = sql.find(name); pos != std::string::npos; pos = sql.find(name, pos + 1))
    {
        const bool left_boundary = pos == 0 || !isIdentifierChar(sql[pos - 1]);
        const size_t end = pos + name.size();
        const bool right_boundary = end >= sql.size() || !isIdentifierChar(sql[end]);
        if (left_boundary && right_boundary)
            return true;
    }
    return false;
}

bool environmentAllowsFixtures(const ContextMutablePtr & context)
{
    /// Read-only sessions (CLI `--readonly`, idempotent HTTP methods, a leaked `SET readonly`) and
    /// sessions with `allow_ddl = 0` must never be used to create or drop scratch objects: these are
    /// caller-owned access gates, so fail closed and let the fixture oracles skip.
    const auto & settings = context->getSettingsRef();
    if (settings[Setting::readonly] != 0 || !settings[Setting::allow_ddl])
        return false;

    const String db = context->getCurrentDatabase();
    if (db.empty() || db == "system" || db == "INFORMATION_SCHEMA" || db == "information_schema")
        return false;

    /// A Replicated database engine turns fixture DDL into replicated log entries — out of scope
    /// and potentially disruptive; skip it.
    try
    {
        auto database = DatabaseCatalog::instance().tryGetDatabase(db);
        if (!database || database->getEngineName() == "Replicated")
            return false;
    }
    catch (...)
    {
        /// Ok: fail-close: an unusable probe result means the oracle cannot evaluate, so skip.
        return false;
    }

    return true;
}
}

OracleFixture::OracleFixture(std::string_view feature_, const ContextMutablePtr & base_context_)
    : base_context(base_context_), feature(feature_)
{
    valid_ = environmentAllowsFixtures(base_context);
}

OracleFixture::~OracleFixture()
{
    if (preserved)
        return;
    /// Drop in reverse creation order. executeStatement is fail-close and never throws, so this is
    /// destructor-safe; a failed drop just leaves the table for the next cleanup pass.
    for (auto it = created.rbegin(); it != created.rend(); ++it)
        OracleExec::executeStatement("DROP TABLE IF EXISTS " + *it + " SYNC", base_context);
}

std::string OracleFixture::allocName(std::string_view suffix)
{
    std::string name = "__oracle_fx_" + feature + "_" + std::to_string(fixture_sequence.fetch_add(1));
    if (!suffix.empty())
    {
        name += "_";
        name += suffix;
    }
    /// Only remembered here; `execute` moves it to `created` once a CREATE naming it succeeds.
    allocated.push_back(name);
    return name;
}

bool OracleFixture::execute(const std::string & sql, const SettingsOverlay & overlay)
{
    if (!OracleExec::executeStatement(sql, base_context, overlay))
        return false;
    armDropsForCreate(sql);
    return true;
}

void OracleFixture::armDropsForCreate(const std::string & sql)
{
    /// Only a successful CREATE that names an allocated table makes this instance the owner of that
    /// table. A bare allocation must not arm a DROP: after a server restart `fixture_sequence` starts
    /// over, so a name can collide with a table `preserve()` intentionally left behind for triage,
    /// and CREATE then fails with TABLE_ALREADY_EXISTS — the preserved table must survive that.
    const size_t start = sql.find_first_not_of(" \t\r\n");
    if (start == std::string::npos || Poco::toUpper(sql.substr(start, 6)) != "CREATE")
        return;
    for (const auto & name : allocated)
    {
        if (armed.contains(name) || !sqlNamesIdentifier(sql, name))
            continue;
        armed.insert(name);
        created.push_back(name);
    }
}

}
