#include <Storages/LiveView/TemporaryLiveViewCleaner.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/LiveView/StorageLiveView.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{
    void executeDropQuery(const StorageID & storage_id, ContextMutablePtr context)
    {
        if (!DatabaseCatalog::instance().isTableExist(storage_id, context))
            return;
        try
        {
            /// We create and execute `drop` query for this table
            auto drop_query = std::make_shared<ASTDropQuery>();
            drop_query->setDatabase(storage_id.database_name);
            drop_query->setTable(storage_id.table_name);
            drop_query->kind = ASTDropQuery::Kind::Drop;
            ASTPtr ast_drop_query = drop_query;
            InterpreterDropQuery drop_interpreter(ast_drop_query, context);
            drop_interpreter.execute();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


std::unique_ptr<TemporaryLiveViewCleaner> TemporaryLiveViewCleaner::the_instance;


void TemporaryLiveViewCleaner::init(ContextMutablePtr global_context_)
{
    if (the_instance)
        throw Exception("TemporaryLiveViewCleaner already initialized", ErrorCodes::LOGICAL_ERROR);
    the_instance.reset(new TemporaryLiveViewCleaner(global_context_));
}

void TemporaryLiveViewCleaner::startup()
{
    background_thread_can_start = true;

    std::lock_guard lock{mutex};
    if (!views.empty())
        startBackgroundThread();
}

void TemporaryLiveViewCleaner::shutdown()
{
    the_instance.reset();
}

TemporaryLiveViewCleaner::TemporaryLiveViewCleaner(ContextMutablePtr global_context_) : WithMutableContext(global_context_)
{
}

TemporaryLiveViewCleaner::~TemporaryLiveViewCleaner()
{
    stopBackgroundThread();
}


void TemporaryLiveViewCleaner::addView(const std::shared_ptr<StorageLiveView> & view)
{
    if (!view->isTemporary() || background_thread_should_exit)
        return;

    auto current_time = std::chrono::system_clock::now();
    auto time_of_next_check = current_time + view->getTimeout();

    /// Keep the vector `views` sorted by time of next check.
    StorageAndTimeOfCheck storage_and_time_of_check{view, time_of_next_check};
    std::lock_guard lock{mutex};
    views.insert(std::upper_bound(views.begin(), views.end(), storage_and_time_of_check), storage_and_time_of_check);

    if (background_thread_can_start)
    {
        startBackgroundThread();
        background_thread_wake_up.notify_one();
    }
}


void TemporaryLiveViewCleaner::backgroundThreadFunc()
{
    std::unique_lock lock{mutex};
    while (!background_thread_should_exit)
    {
        if (views.empty())
            background_thread_wake_up.wait(lock);
        else
            background_thread_wake_up.wait_until(lock, views.front().time_of_check);

        if (background_thread_should_exit)
            break;

        auto current_time = std::chrono::system_clock::now();
        std::vector<StorageID> storages_to_drop;

        auto it = views.begin();
        while (it != views.end())
        {
            std::shared_ptr<StorageLiveView> storage = it->storage.lock();
            auto & time_of_check = it->time_of_check;
            if (!storage)
            {
                /// Storage has been already removed.
                it = views.erase(it);
                continue;
            }

            if (current_time < time_of_check)
                break; /// It's not the time to check it yet.

            auto storage_id = storage->getStorageID();
            if (!storage->hasUsers() && DatabaseCatalog::instance().getDependencies(storage_id).empty())
            {
                /// No users and no dependencies so we can remove the storage.
                storages_to_drop.emplace_back(storage_id);
                it = views.erase(it);
                continue;
            }

            /// Calculate time of the next check.
            time_of_check = current_time + storage->getTimeout();

            ++it;
        }

        lock.unlock();
        for (const auto & storage_id : storages_to_drop)
            executeDropQuery(storage_id, getContext());
        lock.lock();
    }
}


void TemporaryLiveViewCleaner::startBackgroundThread()
{
    if (!background_thread.joinable() && background_thread_can_start && !background_thread_should_exit)
        background_thread = ThreadFromGlobalPool{&TemporaryLiveViewCleaner::backgroundThreadFunc, this};
}

void TemporaryLiveViewCleaner::stopBackgroundThread()
{
    background_thread_should_exit = true;
    background_thread_wake_up.notify_one();
    if (background_thread.joinable())
        background_thread.join();
}

}
