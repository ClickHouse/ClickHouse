#pragma once

#include <Interpreters/QueryOracles/OracleExec.h>
#include <Interpreters/Context_fwd.h>

#include <boost/noncopyable.hpp>

#include <string>
#include <string_view>
#include <vector>

namespace DB
{

/// RAII owner of the tables an oracle creates for a self-seeded / DDL-based check. Scratch mode:
/// it allocates unique, collision-free names and creates nothing until the oracle issues CREATE
/// via `execute`; on destruction it drops every allocated name (DROP TABLE IF EXISTS ... SYNC, in
/// reverse order) unless `preserve()` was called to keep the broken state for triage.
///
/// All statements run under `OracleExec` (i.e. `makeOracleContext`), whose pins guarantee DDL is
/// actually executable (readonly=0, implicit_transaction=0), single-threaded, with no recursive
/// fuzzing. `valid()` is false when the environment cannot host fixtures (system database, etc.);
/// the oracle must skip in that case.
class OracleFixture : private boost::noncopyable
{
public:
    OracleFixture(std::string_view feature, const ContextMutablePtr & base_context);
    ~OracleFixture();

    bool valid() const { return valid_; }

    /// The base (fuzz) context; oracles run their comparison SELECTs through OracleExec with it.
    const ContextMutablePtr & context() const { return base_context; }

    /// Allocate a unique owned table name (registered for drop). `suffix` is a cosmetic tag.
    std::string allocName(std::string_view suffix = {});

    /// Run a CREATE / INSERT / ALTER / OPTIMIZE / SYSTEM statement. Fail-close: false on any error.
    bool execute(const std::string & sql, const SettingsOverlay & overlay = {});

    /// Disarm all drops so the created tables survive for reproduction after a mismatch.
    void preserve() { preserved = true; }

private:
    ContextMutablePtr base_context;
    std::string feature;
    std::vector<std::string> created;   /// owned names, dropped in reverse order
    bool valid_ = false;
    bool preserved = false;
};

}
