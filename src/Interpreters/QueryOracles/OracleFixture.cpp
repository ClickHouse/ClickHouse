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
        if (armed.contains(name) || !sql.contains(name))
            continue;
        armed.insert(name);
        created.push_back(name);
    }
}

}
