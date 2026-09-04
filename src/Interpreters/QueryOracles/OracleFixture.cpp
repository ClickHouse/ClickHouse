#include <Interpreters/QueryOracles/OracleFixture.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>

#include <atomic>

namespace DB
{

namespace
{
/// Process-global sequence for collision-free fixture names (no Date/rand available here).
std::atomic<UInt64> fixture_sequence{0};

bool environmentAllowsFixtures(const ContextMutablePtr & context)
{
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
    created.push_back(name);
    return name;
}

bool OracleFixture::execute(const std::string & sql, const SettingsOverlay & overlay)
{
    return OracleExec::executeStatement(sql, base_context, overlay);
}

}
