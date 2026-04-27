#pragma once

#include <Core/UUID.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

#include <functional>


namespace DB
{

class AccessRights;
class ASTGrantQuery;
struct User;
struct Role;
enum class AccessEntityType : uint8_t;

class InterpreterGrantQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterGrantQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserFromQuery(User & user, const ASTGrantQuery & query);
    static void updateRoleFromQuery(Role & role, const ASTGrantQuery & query);

    /// Runs the same access checks + grantee resolution as `execute`, but instead of
    /// persisting the change clones each grantee, applies the GRANT/REVOKE in-memory,
    /// and invokes `on_grantee` with the simulated post-state. Used by `EXPLAIN GRANT`
    /// / `EXPLAIN REVOKE` to preview the resulting `system.grants` rows without committing.
    /// `ON CLUSTER` is not supported by simulation and is rejected up front.
    using SimulationCallback = std::function<void(const String & grantee_name, AccessEntityType grantee_type, const AccessRights & access)>;
    static void simulate(const ASTPtr & query_ptr, ContextPtr query_context, const SimulationCallback & on_grantee);

private:
    ASTPtr query_ptr;
};

}
