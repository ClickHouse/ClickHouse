#include <Interpreters/InterpreterSystemQuery.h>

#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/SystemCommands/SystemCommandFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/logger_useful.h>


namespace DB
{

InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_->clone())
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();
    const auto & command = SystemCommandFactory::instance().get(query.type);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = command.getRequiredAccessOnCluster(query);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    /// Don't check for constraints when changing profile. It was accepted before (for example it might include
    /// some experimental settings)
    bool check_constraints = false;
    system_context->setCurrentProfile(getContext()->getSystemProfileName(), check_constraints);

    /// Make canonical query for simpler processing
    StorageID table_id = StorageID::createEmpty();
    if (query.type != Type::RELOAD_DICTIONARY && query.type != Type::UNLOAD_DICTIONARY && query.table)
    {
        StorageID id_in_query(query.getDatabase(), query.getTable());
        /// `IF EXISTS` (currently parsed for `SYSTEM SYNC REPLICA`) must suppress
        /// `UNKNOWN_DATABASE` in addition to `UNKNOWN_TABLE`. Plain `resolveStorageID`
        /// throws on a missing database before the per-handler `if_exists` check is
        /// reached, so use `tryResolveStorageID` here when `if_exists` is set.
        /// The handler still validates table existence via the catalog.
        if (query.if_exists)
            table_id = getContext()->tryResolveStorageID(id_in_query, Context::ResolveOrdinary);
        else
            table_id = getContext()->resolveStorageID(id_in_query, Context::ResolveOrdinary);
    }

    VolumePtr volume_ptr;
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

    SystemCommandContext command_context(
        getContext(), std::move(system_context), std::move(table_id), std::move(volume_ptr), getLogger("InterpreterSystemQuery"));
    return command.execute(query, command_context);
}

void registerInterpreterSystemQuery(InterpreterFactory & factory);
void registerInterpreterSystemQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSystemQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSystemQuery", create_fn);
}

}
