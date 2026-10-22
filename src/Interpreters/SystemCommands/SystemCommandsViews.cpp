#include <Interpreters/SystemCommands/SystemCommandHelpers.h>
#include <Interpreters/SystemCommands/SystemCommandsActionLocks.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
}

namespace ActionLocks
{
    extern const StorageActionBlockType ViewRefresh;
    extern const StorageActionBlockType ViewRefreshPause;
    extern const StorageActionBlockType StreamConsume;
}

namespace
{

/// Refreshable materialized views and streaming engines.
class ViewCommands : public SystemCommandContext
{
public:
    explicit ViewCommands(const SystemCommandContext & command_context) : SystemCommandContext(command_context) {}

    void startStopAction(StorageActionBlockType action_type, bool start) { DB::startStopAction(*this, action_type, start); }

    RefreshTaskList getRefreshTasks();
    RefreshTaskList getAccessibleRefreshTasks();
    std::vector<StoragePtr> getAccessibleStreamingStorages();
    void controlBackgroundActivity(const ASTSystemQuery & query);
};

RefreshTaskList ViewCommands::getRefreshTasks()
{
    auto ctx = getContext();
    ctx->checkAccess(AccessType::SYSTEM_VIEWS, table_id);
    auto tasks = ctx->getRefreshSet().findTasks(table_id);
    if (tasks.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Refreshable view {} doesn't exist", table_id.getNameForLogs());
    return tasks;
}

RefreshTaskList ViewCommands::getAccessibleRefreshTasks()
{
    auto ctx = getContext();
    auto access = ctx->getAccess();
    RefreshTaskList result;
    for (const auto & task : ctx->getRefreshSet().getTasks())
    {
        auto view_id = task->getInfo().view_id;
        if (access->isGranted(AccessType::SYSTEM_VIEWS, view_id.database_name, view_id.table_name))
            result.push_back(task);
    }
    return result;
}

std::vector<StoragePtr> ViewCommands::getAccessibleStreamingStorages()
{
    auto ctx = getContext();
    auto access = ctx->getAccess();
    std::vector<StoragePtr> result;
    for (const auto & elem : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false}))
    {
        const auto & database_name = elem.first;
        for (auto iterator = elem.second->getTablesIterator(ctx); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            if (table && table->isStreamingStorage()
                && access->isGranted(AccessType::SYSTEM_STREAMING_ENGINES, database_name, iterator->name()))
                result.push_back(table);
        }
    }
    return result;
}

void ViewCommands::controlBackgroundActivity(const ASTSystemQuery & query)
{
    using Type = ASTSystemQuery::Type;
    const auto type = query.type;

    auto access = getContext()->getAccess();
    const bool can_views = access->isGranted(AccessType::SYSTEM_VIEWS, table_id.database_name, table_id.table_name);
    const bool can_streaming = access->isGranted(AccessType::SYSTEM_STREAMING_ENGINES, table_id.database_name, table_id.table_name);

    auto storage = resolveStorageProxyLoading(DatabaseCatalog::instance().tryGetTable(table_id, getContext()));
    const bool is_streaming = storage && storage->isStreamingStorage();
    const auto * mv = storage ? dynamic_cast<const StorageMaterializedView *>(storage.get()) : nullptr;
    const bool is_refreshable_view = mv && mv->isRefreshable();

    const bool authorized = is_streaming ? can_streaming
        : is_refreshable_view ? can_views
        : storage ? (can_views || can_streaming)
        : (can_views && can_streaming);
    if (!authorized)
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Not enough privileges. To execute this query, it's necessary to have the grant "
            "SYSTEM VIEWS (for refreshable materialized views) or SYSTEM STREAMING ENGINES (for "
            "streaming engines) on {}", table_id.getNameForLogs());

    if (!storage)
        storage = DatabaseCatalog::instance().getTable(table_id, getContext()); /// throws UNKNOWN_TABLE

    if (is_streaming)
    {
        switch (type)
        {
            case Type::STOP:
                startStopAction(ActionLocks::StreamConsume, false);
                storage->cancelBackgroundActivity();
                break;
            case Type::PAUSE:
                startStopAction(ActionLocks::StreamConsume, false);
                break;
            case Type::START:
                startStopAction(ActionLocks::StreamConsume, true);
                break;
            case Type::CANCEL:
                storage->cancelBackgroundActivity();
                break;
            case Type::REFRESH:
                storage->refreshBackgroundActivity();
                break;
            default:
                break;
        }
    }
    else if (is_refreshable_view)
    {
        switch (type)
        {
            case Type::STOP:
                startStopAction(ActionLocks::ViewRefresh, false);
                break;
            case Type::START:
                startStopAction(ActionLocks::ViewRefresh, true);
                startStopAction(ActionLocks::ViewRefreshPause, true);
                break;
            case Type::PAUSE:
                startStopAction(ActionLocks::ViewRefreshPause, false);
                break;
            case Type::CANCEL:
                for (const auto & task : getRefreshTasks())
                    task->cancel();
                break;
            case Type::REFRESH:
                for (const auto & task : getRefreshTasks())
                    task->run();
                break;
            default:
                break;
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table {} has no controllable background activity", table_id.getNameForLogs());
    }
}

}

void registerSystemCommandsViews(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;
    const auto access_on_cluster = tableOrGlobalAccessOnCluster(AccessType::SYSTEM_VIEWS);

    auto register_command = [&](Type type, std::function<void(ASTSystemQuery &, ViewCommands &)> action)
    {
        factory.registerCommand(
            type,
            access_on_cluster,
            [action_fn = std::move(action)](ASTSystemQuery & query, SystemCommandContext & context)
            {
                ViewCommands commands(context);
                action_fn(query, commands);
                return BlockIO{};
            });
    };

    for (const auto type : {Type::START_VIEW, Type::START_VIEWS})
    {
        register_command(type, [](ASTSystemQuery &, ViewCommands & commands)
        {
            /// `SYSTEM START VIEW` must undo both `SYSTEM STOP VIEW` and `SYSTEM PAUSE VIEW`.
            /// Each call drops the corresponding lock (if any) and invokes `refresher->start()`;
            /// `start` is idempotent so calling it twice is safe.
            commands.startStopAction(ActionLocks::ViewRefresh, true);
            commands.startStopAction(ActionLocks::ViewRefreshPause, true);
        });
    }

    for (const auto type : {Type::STOP_VIEW, Type::STOP_VIEWS})
    {
        register_command(type, [](ASTSystemQuery &, ViewCommands & commands)
        {
            commands.startStopAction(ActionLocks::ViewRefresh, false);
        });
    }

    for (const auto type : {Type::PAUSE_VIEW, Type::PAUSE_VIEWS})
    {
        register_command(type, [](ASTSystemQuery &, ViewCommands & commands)
        {
            commands.startStopAction(ActionLocks::ViewRefreshPause, false);
        });
    }

    register_command(Type::START_REPLICATED_VIEW, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getRefreshTasks())
            task->startReplicated();
    });

    register_command(Type::STOP_REPLICATED_VIEW, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getRefreshTasks())
            task->stopReplicated("SYSTEM STOP REPLICATED VIEW");
    });

    register_command(Type::REFRESH_VIEW, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getRefreshTasks())
            task->run();
    });

    register_command(Type::WAIT_VIEW, [](ASTSystemQuery &, ViewCommands & commands)
    {
        auto tasks = commands.getRefreshTasks();
        commands.getContext()->onQueryExecutionStarted();
        for (const auto & task : tasks)
            task->wait(commands.getContext());
    });

    register_command(Type::CANCEL_VIEW, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getRefreshTasks())
            task->cancel();
    });

    register_command(Type::TEST_VIEW, [](ASTSystemQuery & query, ViewCommands & commands)
    {
        /// The parser keeps the literal text; resolving it needs the server timezone.
        std::optional<Int64> fake_time;
        if (query.fake_time_for_view)
        {
            ReadBufferFromString buf(*query.fake_time_for_view);
            time_t time = 0;
            readDateTimeText(time, buf);
            assertEOF(buf);
            fake_time = Int64(time);
        }
        for (const auto & task : commands.getRefreshTasks())
            task->setFakeTime(fake_time);
    });

    /// STOP/START/PAUSE/CANCEL/REFRESH [db.]table | ALL BACKGROUND parser rejects with SYNTAX_ERROR
    /// this is currently unreachable. It is also engine-unaware.
    for (const auto type : {Type::STOP, Type::START, Type::PAUSE, Type::CANCEL, Type::REFRESH})
    {
        register_command(type, [](ASTSystemQuery & query, ViewCommands & commands)
        {
            commands.controlBackgroundActivity(query);
        });
    }

    register_command(Type::STOP_ALL_BACKGROUND, [](ASTSystemQuery &, ViewCommands & commands)
    {
        commands.startStopAction(ActionLocks::ViewRefresh, false);
        commands.startStopAction(ActionLocks::StreamConsume, false);
        for (const auto & streaming_storage : commands.getAccessibleStreamingStorages())
            streaming_storage->cancelBackgroundActivity();
    });

    register_command(Type::START_ALL_BACKGROUND, [](ASTSystemQuery &, ViewCommands & commands)
    {
        commands.startStopAction(ActionLocks::ViewRefresh, true);
        commands.startStopAction(ActionLocks::ViewRefreshPause, true);
        commands.startStopAction(ActionLocks::StreamConsume, true);
    });

    register_command(Type::PAUSE_ALL_BACKGROUND, [](ASTSystemQuery &, ViewCommands & commands)
    {
        commands.startStopAction(ActionLocks::ViewRefreshPause, false);
        commands.startStopAction(ActionLocks::StreamConsume, false);
    });

    register_command(Type::CANCEL_ALL_BACKGROUND, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getAccessibleRefreshTasks())
            task->cancel();
        for (const auto & streaming_storage : commands.getAccessibleStreamingStorages())
            streaming_storage->cancelBackgroundActivity();
    });

    register_command(Type::REFRESH_ALL_BACKGROUND, [](ASTSystemQuery &, ViewCommands & commands)
    {
        for (const auto & task : commands.getAccessibleRefreshTasks())
            task->run();
        for (const auto & streaming_storage : commands.getAccessibleStreamingStorages())
            streaming_storage->refreshBackgroundActivity();
    });
}

}
