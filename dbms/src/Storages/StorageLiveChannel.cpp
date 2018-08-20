/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageLiveView.h>
#include <Storages/StorageLiveChannel.h>
#include <DataStreams/LiveChannelBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TIMEOUT_EXCEEDED;
}

StorageLiveChannel::StorageLiveChannel(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_), global_context(local_context.getGlobalContext()), columns(columns_)
{
    if (!query.tables)
        throw Exception("No tables are specified for channel " + getName(), ErrorCodes::INCORRECT_QUERY);

    String view_database_name;
    String view_table_name;

    for (auto & ast : query.tables->children)
    {
        auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

        if ( table_identifier.children.size() > 2 )
            throw Exception("Invalid table identifier; must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

        if ( table_identifier.children.size() > 1 )
        {
            view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
            view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
        }
        else
        {
            view_database_name = database_name;
            view_table_name = table_identifier.name;
        }

        tables.emplace(std::make_pair(view_database_name, view_table_name));

        global_context.addDependency(
            DatabaseAndTableName(view_database_name, view_table_name),
            DatabaseAndTableName(database_name, table_name));
    }

    is_temporary = query.is_temporary;
    /// start at version 0
    version = std::make_shared<UInt64>(0);
    storage_list_version = std::make_shared<UInt64>(0);
    suspend_list_version = std::make_shared<UInt64>(0);
    resume_list_version = std::make_shared<UInt64>(0);
    refresh_list_version = std::make_shared<UInt64>(0);
    selected_tables_ptr = std::make_shared<StorageListWithLocksPtr>();
    suspend_tables_ptr = std::make_shared<StorageListPtr>();
    resume_tables_ptr = std::make_shared<StorageListPtr>();
    refresh_tables_ptr = std::make_shared<StorageListPtr>();
}

void StorageLiveChannel::noUsersThread()
{
    if (shutdown_called)
        return;

    bool drop_table = false;
    {
        Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
        if (!noUsersThreadWakeUp && !noUsersThreadCondition.tryWait(noUsersThreadMutex, global_context.getSettingsRef().temporary_live_channel_timeout.totalSeconds() * 1000))
        {
            noUsersThreadWakeUp = false;
            if (shutdown_called)
                return;
            if (hasUsers())
                return;
            drop_table = true;
        }
    }
    if (drop_table)
    {
        if ( global_context.tryGetTable(database_name, table_name) )
        {
            try
            {
                /// We create and execute `drop` query for this table
                auto drop_query = std::make_shared<ASTDropQuery>();
                drop_query->database = database_name;
                drop_query->table = table_name;
                ASTPtr ast_drop_query = drop_query;
                InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
                drop_interpreter.execute();
            }
            catch(...)
            {
            }
        }
    }
}

void StorageLiveChannel::startNoUsersThread()
{
    bool expected = false;
    if (!startnousersthread_called.compare_exchange_strong(expected, true))
        return;

    if (is_dropped)
        return;

    if (is_temporary)
    {
        if (no_users_thread.joinable())
        {
            {
                Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
                noUsersThreadWakeUp = true;
                noUsersThreadCondition.signal();
            }
            no_users_thread.join();
        }
        {
            Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
            noUsersThreadWakeUp = false;
        }
        if (!is_dropped)
            no_users_thread = std::thread(&StorageLiveChannel::noUsersThread, this);
    }
    startnousersthread_called = false;
}

void StorageLiveChannel::startup()
{
    startNoUsersThread();
}

void StorageLiveChannel::shutdown()
{
    bool expected = false;
    if (!shutdown_called.compare_exchange_strong(expected, true))
        return;

    if (no_users_thread.joinable())
    {
        Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
        noUsersThreadWakeUp = true;
        noUsersThreadCondition.signal();
        /// Must detach the no users thread
        /// as we can't join it as it will result
        /// in a deadlock
        no_users_thread.detach();
    }
}

StorageLiveChannel::~StorageLiveChannel()
{
    shutdown();
}

void StorageLiveChannel::drop()
{
    for (auto & pair : tables)
    {
        global_context.removeDependency(
            DatabaseAndTableName(pair.first, pair.second),
            DatabaseAndTableName(database_name, table_name));

    }
    Poco::FastMutex::ScopedLock lock(mutex);
    is_dropped = true;
    condition.broadcast();
}

void StorageLiveChannel::setSelectedTables()
{
    Poco::FastMutex::ScopedLock lock(mutex);

    if (!(*selected_tables_ptr))
    {
        auto selected_tables = std::make_shared<StorageListWithLocks>();
        for (auto & table : tables)
        {
            auto storage = global_context.getTable(table.first, table.second);
            if ( storage.get() != this )
                selected_tables->emplace_back(storage, storage->lockStructure(false, __PRETTY_FUNCTION__));
        }

        (*selected_tables_ptr) = selected_tables;
    }

    if (!(*suspend_tables_ptr))
        (*suspend_tables_ptr) = std::make_shared<StorageList>();

    if (!(*resume_tables_ptr))
        (*resume_tables_ptr) = std::make_shared<StorageList>();

    if (!(*refresh_tables_ptr))
        (*refresh_tables_ptr) = std::make_shared<StorageList>();
}

BlockInputStreams StorageLiveChannel::watch(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    const unsigned num_streams)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    /// By default infinite stream of updates
    int64_t length = -2;

    if (query.limit_length)
        length = (int64_t)safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);

    setSelectedTables();

    processed_stage = QueryProcessingStage::Complete;

    auto reader = std::make_shared<LiveChannelBlockInputStream>(*this,
        selected_tables_ptr, suspend_tables_ptr, resume_tables_ptr, refresh_tables_ptr,
        version, storage_list_version, suspend_list_version, resume_list_version, refresh_list_version,
        condition, mutex, length);

    if (no_users_thread.joinable())
    {
        Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
        noUsersThreadWakeUp = true;
        noUsersThreadCondition.signal();
    }

    return { reader };
}

BlockOutputStreamPtr StorageLiveChannel::write(const ASTPtr & query, const Settings & settings)
{
    return std::make_shared<LiveChannelBlockOutputStream>(*this);
}

void StorageLiveChannel::addToChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    NamesAndTypesListPtr new_columns = std::make_shared<NamesAndTypesList>();
    NamesAndTypesList new_materialized_columns{};
    NamesAndTypesList new_alias_columns{};
    ColumnDefaults new_column_defaults{};

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        LiveChannelTables new_tables = tables;
        StorageListPtr new_refresh_tables = std::make_shared<StorageList>();

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            new_tables.emplace(std::make_pair(view_database_name, view_table_name));
            if (storage.get() != this)
                new_refresh_tables->emplace_back(storage);
        }

        auto new_selected_tables = std::make_shared<StorageListWithLocks>();

        std::set<String> all_channel_columns;

        for (auto & table : new_tables)
        {
            auto storage = global_context.getTable(table.first, table.second);

            if ( storage.get() != this )
                new_selected_tables->emplace_back(storage, storage->lockStructure(false, __PRETTY_FUNCTION__));

            NamesAndTypesList columns = storage->getColumnsListNonMaterialized();

            for (auto & pair : columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_columns->insert(new_columns->end(), pair);
            }

            for (auto & pair : storage->materialized_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_materialized_columns.insert(new_materialized_columns.end(), pair);
            }

            for (auto & pair : storage->alias_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_alias_columns.insert(new_alias_columns.end(), pair);
            }

            for (auto & pair : storage->column_defaults)
            {
                if (!all_channel_columns.emplace(pair.first).second)
                    continue;
                new_column_defaults.insert({ pair.first, pair.second });
            }
        }

        IDatabase::ASTModifier ast_modifier;
        ast_modifier = [& new_tables](IAST & ast)
        {
            auto tables_list = std::make_shared<ASTExpressionList>();

            for (auto & table : new_tables)
            {
                ASTPtr database_ast = std::make_shared<ASTIdentifier>(StringRange(), table.first, ASTIdentifier::Database);
                ASTPtr table_ast = std::make_shared<ASTIdentifier>(StringRange(), table.second, ASTIdentifier::Table);

                auto identifier = std::make_shared<ASTIdentifier>(StringRange(), table.first + "." + table.second, ASTIdentifier::Table);
                identifier->children = {database_ast, table_ast};

                tables_list->children.emplace_back(identifier);
            }

            auto & create_query_ast = typeid_cast<ASTCreateQuery &>(ast);
            create_query_ast.set(create_query_ast.tables, tables_list);
        };


        context.getDatabase(database_name)->alterTable(
            context, table_name,
            *new_columns, new_materialized_columns, new_alias_columns, new_column_defaults,
            ast_modifier);

        for (auto & table : new_tables)
        {
            /// if new table is not in tables then add it as dependency
            if ( tables.find(table) == tables.end() )
            {
                global_context.addDependency(
                    DatabaseAndTableName(table.first, table.second),
                    DatabaseAndTableName(database_name, table_name));
            }
        }

        tables = new_tables;
        (*selected_tables_ptr) = new_selected_tables;
        (*storage_list_version)++;

        if (!new_refresh_tables->empty())
        {
            (*refresh_tables_ptr) = new_refresh_tables;
            (*refresh_list_version)++;
        }

        columns = new_columns;
        materialized_columns = new_materialized_columns;
        alias_columns = new_alias_columns;
        column_defaults = new_column_defaults;

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*refresh_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel ADD query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

void StorageLiveChannel::dropFromChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    NamesAndTypesListPtr new_columns = std::make_shared<NamesAndTypesList>();
    NamesAndTypesList new_materialized_columns{};
    NamesAndTypesList new_alias_columns{};
    ColumnDefaults new_column_defaults{};

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    {
        Poco::FastMutex::ScopedLock lock(mutex);
        LiveChannelTables remove_tables;
        LiveChannelTables new_tables = tables;

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Incorrect ALTER query: invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            auto database_table_pair = std::make_pair(view_database_name, view_table_name);

            if ( tables.find(database_table_pair) != tables.end() )
                remove_tables.emplace(database_table_pair);
        }

        if ( remove_tables.size() == tables.size() )
            throw Exception("Incorrect ALTER query: channel would become empty, use DROP TABLE instead", ErrorCodes::INCORRECT_QUERY);

        for (auto & table : remove_tables)
            new_tables.erase(table);

        auto new_selected_tables = std::make_shared<StorageListWithLocks>();

        std::set<String> all_channel_columns;

        for (auto & table : new_tables)
        {
            auto storage = global_context.getTable(table.first, table.second);

            if ( storage.get() != this )
                new_selected_tables->emplace_back(storage, storage->lockStructure(false, __PRETTY_FUNCTION__));

            NamesAndTypesList columns = storage->getColumnsListNonMaterialized();

            for (auto & pair : columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_columns->insert(new_columns->end(), pair);
            }

            for (auto & pair : storage->materialized_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_materialized_columns.insert(new_materialized_columns.end(), pair);
            }

            for (auto & pair : storage->alias_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_alias_columns.insert(new_alias_columns.end(), pair);
            }

            for (auto & pair : storage->column_defaults)
            {
                if (!all_channel_columns.emplace(pair.first).second)
                    continue;
                new_column_defaults.insert({ pair.first, pair.second });
            }
        }

        IDatabase::ASTModifier ast_modifier;
        ast_modifier = [& new_tables](IAST & ast)
        {
            auto tables_list = std::make_shared<ASTExpressionList>();

            for (auto & table : new_tables)
            {
                ASTPtr database_ast = std::make_shared<ASTIdentifier>(StringRange(), table.first, ASTIdentifier::Database);
                ASTPtr table_ast = std::make_shared<ASTIdentifier>(StringRange(), table.second, ASTIdentifier::Table);

                auto identifier = std::make_shared<ASTIdentifier>(StringRange(), table.first + "." + table.second, ASTIdentifier::Table);
                identifier->children = {database_ast, table_ast};

                tables_list->children.emplace_back(identifier);
            }

            auto & create_query_ast = typeid_cast<ASTCreateQuery &>(ast);
            create_query_ast.set(create_query_ast.tables, tables_list);
        };

        context.getDatabase(database_name)->alterTable(
            context, table_name,
            *new_columns, new_materialized_columns, new_alias_columns, new_column_defaults,
            ast_modifier);

        for (auto & table : remove_tables)
            global_context.removeDependency(
                DatabaseAndTableName(table.first, table.second),
                DatabaseAndTableName(database_name, table_name));

        tables = new_tables;
        (*selected_tables_ptr) = new_selected_tables;
        (*storage_list_version)++;

        columns = new_columns;
        materialized_columns = new_materialized_columns;
        alias_columns = new_alias_columns;
        column_defaults = new_column_defaults;

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*selected_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel DROP query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

void StorageLiveChannel::modifyChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    NamesAndTypesListPtr new_columns = std::make_shared<NamesAndTypesList>();
    NamesAndTypesList new_materialized_columns{};
    NamesAndTypesList new_alias_columns{};
    ColumnDefaults new_column_defaults{};

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    {
        Poco::FastMutex::ScopedLock lock(mutex);
        LiveChannelTables new_tables;
        StorageListPtr new_refresh_tables = std::make_shared<StorageList>();

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            new_tables.emplace(std::make_pair(view_database_name, view_table_name));
            if (storage.get() != this)
                new_refresh_tables->emplace_back(storage);
        }

        if (new_tables.size() < 1)
            throw Exception("Incorrect ALTER query, channel would become empty, use DROP TABLE instead", ErrorCodes::INCORRECT_QUERY);

        auto new_selected_tables = std::make_shared<StorageListWithLocks>();

        std::set<String> all_channel_columns;

        for (auto & table : new_tables)
        {
            auto storage = global_context.getTable(table.first, table.second);

            if ( storage.get() != this ) {
                new_selected_tables->emplace_back(storage, storage->lockStructure(false, __PRETTY_FUNCTION__));
                new_refresh_tables->emplace_back(storage);
            }
            NamesAndTypesList columns = storage->getColumnsListNonMaterialized();

            for (auto & pair : columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_columns->insert(new_columns->end(), pair);
            }

            for (auto & pair : storage->materialized_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_materialized_columns.insert(new_materialized_columns.end(), pair);
            }

            for (auto & pair : storage->alias_columns)
            {
                if (!all_channel_columns.emplace(pair.name).second)
                    continue;
                new_alias_columns.insert(new_alias_columns.end(), pair);
            }

            for (auto & pair : storage->column_defaults)
            {
                if (!all_channel_columns.emplace(pair.first).second)
                    continue;
                new_column_defaults.insert({ pair.first, pair.second });
            }
        }

        IDatabase::ASTModifier ast_modifier;
        ast_modifier = [& new_tables](IAST & ast)
        {
            auto tables_list = std::make_shared<ASTExpressionList>();

            for (auto & table : new_tables)
            {
                ASTPtr database_ast = std::make_shared<ASTIdentifier>(StringRange(), table.first, ASTIdentifier::Database);
                ASTPtr table_ast = std::make_shared<ASTIdentifier>(StringRange(), table.second, ASTIdentifier::Table);

                auto identifier = std::make_shared<ASTIdentifier>(StringRange(), table.first + "." + table.second, ASTIdentifier::Table);
                identifier->children = {database_ast, table_ast};

                tables_list->children.emplace_back(identifier);
            }

            auto & create_query_ast = typeid_cast<ASTCreateQuery &>(ast);
            create_query_ast.set(create_query_ast.tables, tables_list);
        };

        context.getDatabase(database_name)->alterTable(
            context, table_name,
            *new_columns, new_materialized_columns, new_alias_columns, new_column_defaults,
            ast_modifier);

        for (auto & table : new_tables)
        {
            /// if new table is not in tables then add it as dependency
            if ( tables.find(table) == tables.end() )
            {
                global_context.addDependency(
                    DatabaseAndTableName(table.first, table.second),
                    DatabaseAndTableName(database_name, table_name));
            }
        }

        for (auto & table: tables)
        {
            /// if table is not in new tables then remove it as dependency
            if (new_tables.find(table) == new_tables.end())
            {
                global_context.removeDependency(
                    DatabaseAndTableName(table.first, table.second),
                    DatabaseAndTableName(database_name, table_name));
            }
        }

        tables = new_tables;
        (*selected_tables_ptr) = new_selected_tables;
        (*storage_list_version)++;

        if (!new_refresh_tables->empty())
        {
            (*refresh_tables_ptr) = new_refresh_tables;
            (*refresh_list_version)++;
        }

        columns = new_columns;
        materialized_columns = new_materialized_columns;
        alias_columns = new_alias_columns;
        column_defaults = new_column_defaults;

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*refresh_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel MODIFY query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

void StorageLiveChannel::refreshInChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    {
        Poco::FastMutex::ScopedLock lock(mutex);
        StorageListPtr new_refresh_tables = std::make_shared<StorageList>();

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            auto database_table_pair = std::make_pair(view_database_name, view_table_name);

            if ( tables.find(database_table_pair) != tables.end() )
                new_refresh_tables->emplace_back(storage);
        }

        if (!new_refresh_tables->empty())
        {
            (*refresh_tables_ptr) = new_refresh_tables;
            (*refresh_list_version)++;
        }

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*refresh_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel REFRESH query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }

}

void StorageLiveChannel::suspendInChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    {
        Poco::FastMutex::ScopedLock lock(mutex);
        StorageList new_suspend_tables = !(*suspend_tables_ptr) ? *std::make_shared<StorageList>() : **suspend_tables_ptr;
        StorageList new_resume_tables = !(*resume_tables_ptr) ? *std::make_shared<StorageList>() : **resume_tables_ptr;

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            auto database_table_pair = std::make_pair(view_database_name, view_table_name);

            if ( tables.find(database_table_pair) != tables.end() )
                new_suspend_tables.emplace_back(storage);

            auto it = std::find(new_resume_tables.begin(), new_resume_tables.end(), storage);
            if (it != new_resume_tables.end())
                new_resume_tables.erase(it);
        }

        new_suspend_tables.unique();

        (*suspend_tables_ptr) = std::make_shared<StorageList>(std::move(new_suspend_tables));
        (*suspend_list_version)++;
        (*resume_tables_ptr) = std::make_shared<StorageList>(std::move(new_resume_tables));
        (*resume_list_version)++;

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*suspend_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel SUSPEND query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

void StorageLiveChannel::resumeInChannel(const ASTPtr & values, const Context & context)
{
    String view_database_name;
    String view_table_name;

    auto alter_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    {
        Poco::FastMutex::ScopedLock lock(mutex);
        StorageList new_suspend_tables = !(*suspend_tables_ptr) ? *std::make_shared<StorageList>() : **suspend_tables_ptr;
        StorageList new_resume_tables = !(*resume_tables_ptr) ? *std::make_shared<StorageList>() : **resume_tables_ptr;

        for (auto & ast : values->children)
        {
            auto & table_identifier = typeid_cast<ASTIdentifier &>(*ast);

            if ( table_identifier.children.size() > 2 )
                throw Exception("Incorrect ALTER query: invalid table identifier must be of the form [db.]name", ErrorCodes::INCORRECT_QUERY);

            if ( table_identifier.children.size() > 1 )
            {
                view_database_name = static_cast<const ASTIdentifier &>(*table_identifier.children[0].get()).name;
                view_table_name = static_cast<const ASTIdentifier &>(*table_identifier.children[1].get()).name;
            }
            else
            {
                view_database_name = database_name;
                view_table_name = table_identifier.name;
            }

            auto storage = global_context.getTable(view_database_name, view_table_name);

            if ( !(std::dynamic_pointer_cast<StorageLiveView>(storage)) )
                throw Exception("Invalid table storage, must be StorageLiveView", ErrorCodes::INCORRECT_QUERY);

            auto database_table_pair = std::make_pair(view_database_name, view_table_name);

            if ( tables.find(database_table_pair) != tables.end() )
                new_resume_tables.emplace_back(storage);

            auto it = std::find(new_suspend_tables.begin(), new_suspend_tables.end(), storage);
            if (it != new_suspend_tables.end())
                new_suspend_tables.erase(it);
        }

        new_resume_tables.unique();

        (*suspend_tables_ptr) = std::make_shared<StorageList>(std::move(new_suspend_tables));
        (*suspend_list_version)++;
        (*resume_tables_ptr) = std::make_shared<StorageList>(std::move(new_resume_tables));
        (*resume_list_version)++;

        /// notify all readers
        condition.broadcast();
    }

    std::chrono::milliseconds wait_ms(context.getSettingsRef().alter_channel_wait_ms.totalMilliseconds());
    std::chrono::milliseconds sleep_for(50);

    while (version.use_count() != (*resume_tables_ptr).use_count())
    {
        std::this_thread::sleep_for(sleep_for);
        wait_ms -= sleep_for;
        if (wait_ms <= std::chrono::milliseconds(0))
            throw Exception("Channel RESUME query timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

}
