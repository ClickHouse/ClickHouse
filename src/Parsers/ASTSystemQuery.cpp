#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Poco/String.h>
#include <Common/quoteString.h>
#include <Interpreters/InstrumentationManager.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    std::vector<std::string> getTypeIndexToTypeName()
    {
        constexpr std::size_t types_size = magic_enum::enum_count<ASTSystemQuery::Type>();

        std::vector<std::string> type_index_to_type_name;
        type_index_to_type_name.resize(types_size);

        auto entries = magic_enum::enum_entries<ASTSystemQuery::Type>();
        for (const auto & [entry, str] : entries)
        {
            auto str_copy = String(str);
            std::replace(str_copy.begin(), str_copy.end(), '_', ' ');
            type_index_to_type_name[static_cast<UInt64>(entry)] = std::move(str_copy);
        }

        return type_index_to_type_name;
    }
}

const char * ASTSystemQuery::typeToString(Type type)
{
    /** During parsing if SystemQuery is not parsed properly it is added to Expected variants as description check IParser.h.
      * Description string must be statically allocated.
      */
    static std::vector<std::string> type_index_to_type_name = getTypeIndexToTypeName();
    const auto & type_name = type_index_to_type_name[static_cast<UInt64>(type)];
    return type_name.data();
}

String ASTSystemQuery::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTSystemQuery::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTSystemQuery::setDatabase(const String & name)
{
    reset(database);
    if (!name.empty())
        set(database, make_intrusive<ASTIdentifier>(name));
}

void ASTSystemQuery::setTable(const String & name)
{
    reset(table);
    if (!name.empty())
        set(table, make_intrusive<ASTIdentifier>(name));
}

void ASTSystemQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    auto print_identifier = [&](const String & identifier) -> WriteBuffer &
    {
        ostr << backQuoteIfNeed(identifier)
                     ;
        return ostr;
    };

    auto print_keyword = [&](const auto & keyword) -> WriteBuffer &
    {
        ostr << keyword;
        return ostr;
    };

    auto print_database_table = [&]() -> WriteBuffer &
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);

        if (if_exists)
            print_keyword(" IF EXISTS");

        return ostr;
    };

    auto print_restore_database_replica = [&]() -> WriteBuffer &
    {
        chassert(database);

        ostr << " ";
        print_identifier(getDatabase());

        return ostr;
    };

    auto print_drop_replica = [&]
    {
        ostr << " " << quoteString(replica);
        if (!shard.empty())
            print_keyword(" FROM SHARD ") << quoteString(shard);

        if (table)
        {
            print_keyword(" FROM TABLE ");
            print_database_table();
        }
        else if (!full_replica_zk_path.empty())
        {
            print_keyword(" FROM ZKPATH ") << quoteString(full_replica_zk_path);
        }
        else if (database)
        {
            print_keyword(" FROM DATABASE ");
            print_identifier(getDatabase());

            if (with_tables)
                print_keyword(" WITH TABLES");
        }
    };

    auto print_on_volume = [&]
    {
        print_keyword(" ON VOLUME ");
        print_identifier(storage_policy) << ".";
        print_identifier(volume);
    };

    print_keyword("SYSTEM") << " ";
    print_keyword(typeToString(type));

    std::unordered_set<Type> queries_with_on_cluster_at_end = {
        Type::CLEAR_FILESYSTEM_CACHE,
        Type::SYNC_FILESYSTEM_CACHE,
        Type::CLEAR_QUERY_CACHE,
    };

    if (!queries_with_on_cluster_at_end.contains(type) && !cluster.empty())
        formatOnCluster(ostr, settings);

    switch (type)
    {
        case Type::STOP_MERGES:
        case Type::START_MERGES:
        case Type::STOP_TTL_MERGES:
        case Type::START_TTL_MERGES:
        case Type::STOP_MOVES:
        case Type::START_MOVES:
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATED_SENDS:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        case Type::STOP_PULLING_REPLICATION_LOG:
        case Type::START_PULLING_REPLICATION_LOG:
        case Type::STOP_CLEANUP:
        case Type::START_CLEANUP:
        case Type::LOAD_PRIMARY_KEY:
        case Type::UNLOAD_PRIMARY_KEY:
        case Type::STOP_VIRTUAL_PARTS_UPDATE:
        case Type::START_VIRTUAL_PARTS_UPDATE:
        case Type::STOP_REDUCE_BLOCKING_PARTS:
        case Type::START_REDUCE_BLOCKING_PARTS:
        case Type::SYNC_MERGES:
        {
            if (table)
            {
                ostr << ' ';
                print_database_table();
            }
            else if (!volume.empty())
            {
                print_on_volume();
            }
            break;
        }
        case Type::SCHEDULE_MERGE:
        {
            chassert(table);
            ostr << ' ';
            print_database_table();
            print_keyword(" PARTS ");
            chassert(scheduled_merge_parts);
            scheduled_merge_parts->format(ostr, settings, state, frame);
            break;
        }
        case Type::FLUSH_OBJECT_STORAGE_QUEUE:
        {
            ostr << ' ';
            print_database_table();
            ostr << " PATH " << quoteString(queue_path);
            break;
        }
        case Type::RESTART_REPLICA:
        case Type::RESTORE_REPLICA:
        case Type::SYNC_REPLICA:
        case Type::WAIT_LOADING_PARTS:
        case Type::FLUSH_DISTRIBUTED:
        case Type::PREWARM_MARK_CACHE:
        case Type::PREWARM_PRIMARY_INDEX_CACHE:
        {
            if (table)
            {
                ostr << ' ';
                print_database_table();
            }

            if (sync_replica_mode != SyncReplicaMode::DEFAULT)
            {
                ostr << ' ';
                print_keyword(magic_enum::enum_name(sync_replica_mode));

                // If the mode is LIGHTWEIGHT and specific source replicas are specified
                if (sync_replica_mode == SyncReplicaMode::LIGHTWEIGHT && !src_replicas.empty())
                {
                    ostr << ' ';
                    print_keyword("FROM");
                    ostr << ' ';

                    bool first = true;
                    for (const auto & src : src_replicas)
                    {
                        if (!first)
                            ostr << ", ";
                        first = false;
                        ostr << quoteString(src);
                    }
                }
            }

            if (query_settings)
            {
                ostr << settings.nl_or_ws << "SETTINGS ";
                query_settings->format(ostr, settings, state, frame);
            }

            break;
        }
        case Type::RELOAD_DICTIONARY:
        case Type::RELOAD_MODEL:
        case Type::RELOAD_FUNCTION:
        case Type::RESTART_DISK:
        case Type::WAIT_BLOBS_CLEANUP:
        case Type::CLEAR_DISK_METADATA_CACHE:
        {
            if (table)
            {
                ostr << ' ';
                print_database_table();
            }
            else if (!target_model.empty())
            {
                ostr << ' ';
                print_identifier(target_model);
            }
            else if (!target_function.empty())
            {
                ostr << ' ';
                print_identifier(target_function);
            }
            else if (!disk.empty())
            {
                ostr << ' ';
                print_identifier(disk);
            }

            break;
        }
        case Type::SYNC_DATABASE_REPLICA:
        {
            ostr << ' ';
            database->format(ostr, settings, state, frame);
            if (sync_replica_mode != SyncReplicaMode::DEFAULT)
            {
                ostr << ' ';
                print_keyword(magic_enum::enum_name(sync_replica_mode));
            }
            break;
        }
        case Type::DROP_REPLICA:
        case Type::DROP_DATABASE_REPLICA:
        case Type::DROP_CATALOG_REPLICA:
        {
            print_drop_replica();
            break;
        }
        case Type::RESTORE_DATABASE_REPLICA:
        {
            if (database)
            {
                print_restore_database_replica();
            }
            break;
        }
        case Type::SUSPEND:
        {
            print_keyword(" FOR ") << seconds;
            print_keyword(" SECOND");
            break;
        }
        case Type::CLEAR_FORMAT_SCHEMA_CACHE:
        {
            if (!schema_cache_format.empty())
            {
                print_keyword(" FOR ");
                print_identifier(schema_cache_format);
            }
            break;
        }
        case Type::CLEAR_FILESYSTEM_CACHE:
        {
            if (!filesystem_cache_name.empty())
            {
                ostr << ' ' << quoteString(filesystem_cache_name);
                if (!key_to_drop.empty())
                {
                    print_keyword(" KEY ");
                    print_identifier(key_to_drop);
                    if (offset_to_drop.has_value())
                    {
                        print_keyword(" OFFSET ");
                        ostr << offset_to_drop.value();
                    }
                }
            }
            break;
        }
        case Type::CLEAR_SCHEMA_CACHE:
        {
            if (!schema_cache_storage.empty())
            {
                print_keyword(" FOR ");
                print_identifier(schema_cache_storage);
            }
            break;
        }
        case Type::CLEAR_DISTRIBUTED_CACHE:
        {
            if (distributed_cache_drop_connections)
                print_keyword(" CONNECTIONS");
            else if (!distributed_cache_server_id.empty())
                ostr << " " << distributed_cache_server_id;
            break;
        }
        case Type::CLEAR_QUERY_CACHE:
        {
            if (query_result_cache_tag.has_value())
            {
                print_keyword(" TAG ");
                ostr << quoteString(*query_result_cache_tag);
            }
            break;
        }
        case Type::UNFREEZE:
        {
            print_keyword(" WITH NAME ");
            ostr << quoteString(backup_name);
            break;
        }
        case Type::UNLOCK_SNAPSHOT:
        {
            ostr << " " << quoteString(backup_name);
            if (backup_source)
            {
                print_keyword(" FROM ");
                backup_source->format(ostr, settings);
            }
            break;
        }
        case Type::START_LISTEN:
        case Type::STOP_LISTEN:
        {
            ostr << ' ';
            print_keyword(ServerType::serverTypeToString(server_type.type));

            if (server_type.type == ServerType::Type::CUSTOM)
                ostr << ' ' << quoteString(server_type.custom_name);

            bool comma = false;

            if (!server_type.exclude_types.empty())
            {
                print_keyword(" EXCEPT");

                for (auto cur_type : server_type.exclude_types)
                {
                    if (cur_type == ServerType::Type::CUSTOM)
                        continue;

                    if (comma)
                        ostr << ',';
                    else
                        comma = true;

                    ostr << ' ';
                    print_keyword(ServerType::serverTypeToString(cur_type));
                }

                if (server_type.exclude_types.contains(ServerType::Type::CUSTOM))
                {
                    for (const auto & cur_name : server_type.exclude_custom_names)
                    {
                        if (comma)
                            ostr << ',';
                        else
                            comma = true;

                        ostr << ' ';
                        print_keyword(ServerType::serverTypeToString(ServerType::Type::CUSTOM));
                        ostr << " " << quoteString(cur_name);
                    }
                }
            }
            break;
        }
        case Type::SET_COVERAGE_TEST:
        {
            ostr << ' ';
            ostr << quoteString(coverage_test_name);
            break;
        }
        case Type::ENABLE_FAILPOINT:
        case Type::DISABLE_FAILPOINT:
        case Type::NOTIFY_FAILPOINT:
        {
            ostr << ' ';
            print_identifier(fail_point_name);
            break;
        }
        case Type::WAIT_FAILPOINT:
        {
            ostr << ' ';
            print_identifier(fail_point_name);
            if (fail_point_action == FailPointAction::PAUSE)
            {
                ostr << ' ';
                print_keyword("PAUSE");
            }
            else if (fail_point_action == FailPointAction::RESUME)
            {
                ostr << ' ';
                print_keyword("RESUME");
            }
            break;
        }
        case Type::REFRESH_VIEW:
        case Type::START_VIEW:
        case Type::START_REPLICATED_VIEW:
        case Type::STOP_VIEW:
        case Type::STOP_REPLICATED_VIEW:
        case Type::PAUSE_VIEW:
        case Type::CANCEL_VIEW:
        case Type::WAIT_VIEW:
        {
            ostr << ' ';
            print_database_table();
            break;
        }
        case Type::TEST_VIEW:
        {
            ostr << ' ';
            print_database_table();

            if (!fake_time_for_view)
            {
                ostr << ' ';
                print_keyword("UNSET FAKE TIME");
            }
            else
            {
                ostr << ' ';
                print_keyword("SET FAKE TIME");
                ostr << " '" << LocalDateTime(*fake_time_for_view) << "'";
            }
            break;
        }
        case Type::RELOAD_DELTA_KERNEL_TRACING:
        {
            ostr << ' ';
            print_identifier(delta_kernel_tracing_level);
            break;
        }
        case Type::FLUSH_ASYNC_INSERT_QUEUE:
        case Type::FLUSH_LOGS:
        {
            bool comma = false;
            for (const auto & cur_log : tables)
            {
                if (comma)
                    ostr << ',';
                else
                    comma = true;
                ostr << ' ';

                if (!cur_log.first.empty())
                    print_identifier(cur_log.first) << ".";
                print_identifier(cur_log.second);
            }
            break;
        }

        case Type::ALLOCATE_MEMORY:
        {
            ostr << ' ' << untracked_memory_size;
            break;
        }

#if USE_XRAY
        case Type::INSTRUMENT_ADD:
        {
            if (!instrumentation_function_name.empty())
                ostr << ' ' << quoteString(instrumentation_function_name);

            if (!instrumentation_handler_name.empty())
            {
                ostr << ' ';
                print_identifier(Poco::toUpper(instrumentation_handler_name));
            }

            switch (instrumentation_entry_type)
            {
                case Instrumentation::EntryType::ENTRY:
                    ostr << " ENTRY"; break;
                case Instrumentation::EntryType::EXIT:
                    ostr << " EXIT"; break;
                case Instrumentation::EntryType::ENTRY_AND_EXIT:
                    break;
            }

            for (const auto & arg : instrumentation_arguments)
            {
                std::visit([&](const auto & value)
                {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, String>)
                        ostr << ' ' << quoteString(value);
                    else
                        ostr << ' ' << value;
                }, arg);
            }
            break;
        }
        case Type::INSTRUMENT_REMOVE:
        {
            if (!instrumentation_subquery.empty())
                ostr << " (" << instrumentation_subquery << ')';
            else if (instrumentation_point)
            {
                if (std::holds_alternative<Instrumentation::All>(instrumentation_point.value()))
                    ostr << " ALL";
                else if (std::holds_alternative<String>(instrumentation_point.value()))
                    ostr << ' ' << quoteString(std::get<String>(instrumentation_point.value()));
                else
                    ostr << ' ' << std::get<UInt64>(instrumentation_point.value());
            }
            break;
        }
#else
        case Type::INSTRUMENT_ADD:
        case Type::INSTRUMENT_REMOVE:
#endif

        case Type::KILL:
        case Type::SHUTDOWN:
        case Type::CLEAR_DNS_CACHE:
        case Type::CLEAR_CONNECTIONS_CACHE:
        case Type::CLEAR_MMAP_CACHE:
        case Type::CLEAR_QUERY_CONDITION_CACHE:
        case Type::CLEAR_MARK_CACHE:
        case Type::CLEAR_PRIMARY_INDEX_CACHE:
        case Type::CLEAR_INDEX_MARK_CACHE:
        case Type::CLEAR_UNCOMPRESSED_CACHE:
        case Type::CLEAR_INDEX_UNCOMPRESSED_CACHE:
        case Type::CLEAR_VECTOR_SIMILARITY_INDEX_CACHE:
        case Type::CLEAR_TEXT_INDEX_TOKENS_CACHE:
        case Type::CLEAR_TEXT_INDEX_HEADER_CACHE:
        case Type::CLEAR_TEXT_INDEX_POSTINGS_CACHE:
        case Type::CLEAR_TEXT_INDEX_CACHES:
        case Type::CLEAR_COMPILED_EXPRESSION_CACHE:
        case Type::CLEAR_S3_CLIENT_CACHE:
        case Type::CLEAR_ICEBERG_METADATA_CACHE:
        case Type::CLEAR_PARQUET_METADATA_CACHE:
        case Type::CLEAR_AVRO_SCHEMA_CACHE:
        case Type::RESET_COVERAGE:
        case Type::RESTART_REPLICAS:
        case Type::JEMALLOC_PURGE:
        case Type::JEMALLOC_FLUSH_PROFILE:
        case Type::JEMALLOC_ENABLE_PROFILE:
        case Type::JEMALLOC_DISABLE_PROFILE:
        case Type::SYNC_TRANSACTION_LOG:
        case Type::SYNC_FILE_CACHE:
        case Type::REPLICA_READY:   /// Obsolete
        case Type::REPLICA_UNREADY: /// Obsolete
        case Type::RELOAD_DICTIONARIES:
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        case Type::RELOAD_MODELS:
        case Type::RELOAD_FUNCTIONS:
        case Type::RELOAD_CONFIG:
        case Type::RELOAD_USERS:
        case Type::RELOAD_ASYNCHRONOUS_METRICS:
        case Type::START_THREAD_FUZZER:
        case Type::STOP_THREAD_FUZZER:
        case Type::START_VIEWS:
        case Type::STOP_VIEWS:
        case Type::PAUSE_VIEWS:
        case Type::CLEAR_PAGE_CACHE:
        case Type::STOP_REPLICATED_DDL_QUERIES:
        case Type::START_REPLICATED_DDL_QUERIES:
        case Type::RECONNECT_ZOOKEEPER:
        case Type::FREE_MEMORY:
        case Type::RESET_DDL_WORKER:
            break;
        case Type::SYNC_FILESYSTEM_CACHE:
        {
            if (!filesystem_cache_name.empty())
                ostr << ' ' << quoteString(filesystem_cache_name);
            break;
        }
        case Type::UNKNOWN:
        case Type::END:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown SYSTEM command");
    }

    if (queries_with_on_cluster_at_end.contains(type) && !cluster.empty())
        formatOnCluster(ostr, settings);
}

void ASTSystemQuery::writeJSON(WriteBuffer & out) const
{
#if USE_XRAY
    if (type == Type::INSTRUMENT_ADD || type == Type::INSTRUMENT_REMOVE)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JSON serialization is not supported for SYSTEM INSTRUMENT queries");
#endif
    JSONObjectWriter w(out, "SystemQuery");
    w.writeInt("query_type", static_cast<Int64>(type));
    w.writeString("query_type_name", typeToString(type));
    w.writeChild("database", database);
    w.writeChild("table", table);
    if (if_exists)
        w.writeBool("if_exists", true);
    w.writeChild("query_settings", query_settings);
    if (!target_model.empty())
        w.writeString("target_model", target_model);
    if (!target_function.empty())
        w.writeString("target_function", target_function);
    if (!replica.empty())
        w.writeString("replica", replica);
    if (!shard.empty())
        w.writeString("shard", shard);
    if (!zk_name.empty())
        w.writeString("zk_name", zk_name);
    if (!full_replica_zk_path.empty())
        w.writeString("full_replica_zk_path", full_replica_zk_path);
    if (!replica_zk_path.empty())
        w.writeString("replica_zk_path", replica_zk_path);
    if (is_drop_whole_replica)
        w.writeBool("is_drop_whole_replica", true);
    if (with_tables)
        w.writeBool("with_tables", true);
    if (!storage_policy.empty())
        w.writeString("storage_policy", storage_policy);
    if (!volume.empty())
        w.writeString("volume", volume);
    if (!disk.empty())
        w.writeString("disk", disk);
    if (seconds != 0)
        w.writeUInt("seconds", seconds);
    if (untracked_memory_size != 0)
        w.writeUInt("untracked_memory_size", untracked_memory_size);
    if (query_result_cache_tag.has_value())
        w.writeString("query_result_cache_tag", *query_result_cache_tag);
    if (!filesystem_cache_name.empty())
        w.writeString("filesystem_cache_name", filesystem_cache_name);
    if (!distributed_cache_server_id.empty())
        w.writeString("distributed_cache_server_id", distributed_cache_server_id);
    if (distributed_cache_drop_connections)
        w.writeBool("distributed_cache_drop_connections", true);
    if (!key_to_drop.empty())
        w.writeString("key_to_drop", key_to_drop);
    if (offset_to_drop.has_value())
        w.writeUInt("offset_to_drop", *offset_to_drop);
    if (!backup_name.empty())
        w.writeString("backup_name", backup_name);
    w.writeChild("backup_source", backup_source);
    w.writeChild("scheduled_merge_parts", scheduled_merge_parts);
    if (!schema_cache_storage.empty())
        w.writeString("schema_cache_storage", schema_cache_storage);
    if (!schema_cache_format.empty())
        w.writeString("schema_cache_format", schema_cache_format);
    if (!queue_path.empty())
        w.writeString("queue_path", queue_path);
    if (!fail_point_name.empty())
        w.writeString("fail_point_name", fail_point_name);
    if (fail_point_action != FailPointAction::UNSPECIFIED)
        w.writeInt("fail_point_action", static_cast<Int64>(fail_point_action));
    if (!delta_kernel_tracing_level.empty())
        w.writeString("delta_kernel_tracing_level", delta_kernel_tracing_level);
    if (!coverage_test_name.empty())
        w.writeString("coverage_test_name", coverage_test_name);
    if (sync_replica_mode != SyncReplicaMode::DEFAULT)
        w.writeInt("sync_replica_mode", static_cast<Int64>(sync_replica_mode));
    if (!src_replicas.empty())
    {
        w.writeKey("src_replicas");
        auto & buf = w.getOut();
        buf << '[';
        for (size_t i = 0; i < src_replicas.size(); ++i)
        {
            if (i > 0) buf << ',';
            writeJSONString(src_replicas[i], buf, w.getFormatSettings());
        }
        buf << ']';
    }
    if (fake_time_for_view.has_value())
        w.writeInt("fake_time_for_view", *fake_time_for_view);
    if (!tables.empty())
    {
        w.writeKey("tables");
        auto & buf = w.getOut();
        buf << '[';
        for (size_t i = 0; i < tables.size(); ++i)
        {
            if (i > 0) buf << ',';
            buf << "{\"database\":";
            writeJSONString(tables[i].first, buf, w.getFormatSettings());
            buf << ",\"table\":";
            writeJSONString(tables[i].second, buf, w.getFormatSettings());
            buf << '}';
        }
        buf << ']';
    }
    if (type == Type::STOP_LISTEN || type == Type::START_LISTEN)
    {
        w.writeKey("server_type");
        auto & buf = w.getOut();
        buf << "{\"type\":" << static_cast<Int64>(server_type.type);
        if (!server_type.custom_name.empty())
        {
            buf << ",\"custom_name\":";
            writeJSONString(server_type.custom_name, buf, w.getFormatSettings());
        }
        if (!server_type.exclude_types.empty())
        {
            buf << ",\"exclude_types\":[";
            bool first_excl = true;
            for (auto t : server_type.exclude_types)
            {
                if (!first_excl) buf << ',';
                first_excl = false;
                buf << static_cast<Int64>(t);
            }
            buf << ']';
        }
        if (!server_type.exclude_custom_names.empty())
        {
            buf << ",\"exclude_custom_names\":[";
            bool first_excl = true;
            for (const auto & n : server_type.exclude_custom_names)
            {
                if (!first_excl) buf << ',';
                first_excl = false;
                writeJSONString(n, buf, w.getFormatSettings());
            }
            buf << ']';
        }
        buf << '}';
    }
    if (!cluster.empty())
        w.writeString("cluster", cluster);
    w.writeChildren(children);
}

void ASTSystemQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    if (!r.has("query_type"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'query_type' field in `SystemQuery` during AST JSON deserialization");
    Int64 query_type_value = r.getInt("query_type");
    auto query_type_opt = magic_enum::enum_cast<Type>(static_cast<std::underlying_type_t<Type>>(query_type_value));
    if (!query_type_opt || static_cast<Int64>(*query_type_opt) != query_type_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SYSTEM query_type: {}", query_type_value);
    type = *query_type_opt;
#if USE_XRAY
    if (type == Type::INSTRUMENT_ADD || type == Type::INSTRUMENT_REMOVE)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JSON serialization is not supported for SYSTEM INSTRUMENT queries");
#endif
    database = r.readChild("database");
    if (database)
        children.push_back(database);
    table = r.readChild("table");
    if (table)
        children.push_back(table);
    if_exists = r.getBool("if_exists");
    query_settings = r.readChild("query_settings");
    if (query_settings)
        children.push_back(query_settings);
    target_model = r.getString("target_model");
    target_function = r.getString("target_function");
    replica = r.getString("replica");
    shard = r.getString("shard");
    zk_name = r.getString("zk_name");
    full_replica_zk_path = r.getString("full_replica_zk_path");
    replica_zk_path = r.getString("replica_zk_path");
    is_drop_whole_replica = r.getBool("is_drop_whole_replica");
    with_tables = r.getBool("with_tables");
    storage_policy = r.getString("storage_policy");
    volume = r.getString("volume");
    disk = r.getString("disk");
    seconds = r.getUInt("seconds");
    untracked_memory_size = r.getUInt("untracked_memory_size");
    if (r.has("query_result_cache_tag"))
        query_result_cache_tag = r.getString("query_result_cache_tag");
    filesystem_cache_name = r.getString("filesystem_cache_name");
    distributed_cache_server_id = r.getString("distributed_cache_server_id");
    distributed_cache_drop_connections = r.getBool("distributed_cache_drop_connections");
    key_to_drop = r.getString("key_to_drop");
    if (r.has("offset_to_drop"))
        offset_to_drop = r.getUInt("offset_to_drop");
    backup_name = r.getString("backup_name");
    backup_source = r.readChild("backup_source");
    if (backup_source)
        children.push_back(backup_source);
    scheduled_merge_parts = r.readChild("scheduled_merge_parts");
    if (scheduled_merge_parts)
        children.push_back(scheduled_merge_parts);
    schema_cache_storage = r.getString("schema_cache_storage");
    schema_cache_format = r.getString("schema_cache_format");
    queue_path = r.getString("queue_path");
    fail_point_name = r.getString("fail_point_name");
    if (r.has("fail_point_action"))
    {
        Int64 action_value = r.getInt("fail_point_action");
        auto action_opt = magic_enum::enum_cast<FailPointAction>(static_cast<std::underlying_type_t<FailPointAction>>(action_value));
        if (!action_opt || static_cast<Int64>(*action_opt) != action_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SYSTEM fail_point_action: {}", action_value);
        fail_point_action = *action_opt;
    }
    delta_kernel_tracing_level = r.getString("delta_kernel_tracing_level");
    coverage_test_name = r.getString("coverage_test_name");
    if (r.has("sync_replica_mode"))
    {
        Int64 mode_value = r.getInt("sync_replica_mode");
        auto mode_opt = magic_enum::enum_cast<SyncReplicaMode>(static_cast<std::underlying_type_t<SyncReplicaMode>>(mode_value));
        if (!mode_opt || static_cast<Int64>(*mode_opt) != mode_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SYSTEM sync_replica_mode: {}", mode_value);
        sync_replica_mode = *mode_opt;
    }
    src_replicas = r.readStringArray("src_replicas");
    if (r.has("fake_time_for_view"))
        fake_time_for_view = r.getInt("fake_time_for_view");
    if (r.has("tables"))
    {
        auto arr = r.getArray("tables");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tables' is not a JSON array");
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto t_obj = arr->getObject(i);
            if (!t_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'tables' array during AST JSON deserialization", i);
            tables.emplace_back(
                t_obj->getValue<String>("database"),
                t_obj->getValue<String>("table"));
        }
    }
    /// Validate per-`type` required fields so the deserialized AST cannot reach
    /// a null-dereference path in `formatImpl`.
    switch (type)
    {
        case Type::SCHEDULE_MERGE:
            if (!table)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`SYSTEM SCHEDULE_MERGE` requires 'table' during AST JSON deserialization");
            if (!scheduled_merge_parts)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`SYSTEM SCHEDULE_MERGE` requires 'scheduled_merge_parts' during AST JSON deserialization");
            break;
        case Type::FLUSH_OBJECT_STORAGE_QUEUE:
        case Type::REFRESH_VIEW:
        case Type::START_VIEW:
        case Type::START_REPLICATED_VIEW:
        case Type::STOP_VIEW:
        case Type::STOP_REPLICATED_VIEW:
        case Type::PAUSE_VIEW:
        case Type::CANCEL_VIEW:
        case Type::WAIT_VIEW:
        case Type::TEST_VIEW:
            if (!table)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`SYSTEM {}` requires 'table' during AST JSON deserialization", typeToString(type));
            break;
        case Type::SYNC_DATABASE_REPLICA:
            if (!database)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`SYSTEM SYNC_DATABASE_REPLICA` requires 'database' during AST JSON deserialization");
            break;
        case Type::START_LISTEN:
        case Type::STOP_LISTEN:
            /// `formatImpl` unconditionally reads `server_type.type` for these queries.
            /// Without a 'server_type' field the AST would carry a default-constructed
            /// `ServerType` and silently format as an unrelated server type, so require it.
            if (!r.has("server_type"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`SYSTEM {}` requires 'server_type' during AST JSON deserialization", typeToString(type));
            break;
        default:
            break;
    }

    if (r.has("server_type"))
    {
        auto srv_obj = r.getNestedObject("server_type");
        if (!srv_obj)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'server_type' is not a JSON object");
        Int64 srv_type_value = srv_obj->getValue<Poco::Int64>("type");
        auto srv_type_opt = magic_enum::enum_cast<ServerType::Type>(static_cast<std::underlying_type_t<ServerType::Type>>(srv_type_value));
        if (!srv_type_opt || static_cast<Int64>(*srv_type_opt) != srv_type_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SYSTEM server_type.type: {}", srv_type_value);
        server_type.type = *srv_type_opt;
        if (srv_obj->has("custom_name"))
            server_type.custom_name = srv_obj->getValue<String>("custom_name");
        if (srv_obj->has("exclude_types"))
        {
            auto arr = srv_obj->getArray("exclude_types");
            if (!arr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'server_type.exclude_types' is not a JSON array");
            for (unsigned int i = 0; i < arr->size(); ++i)
            {
                Int64 v = arr->getElement<Poco::Int64>(i);
                auto opt = magic_enum::enum_cast<ServerType::Type>(static_cast<std::underlying_type_t<ServerType::Type>>(v));
                if (!opt || static_cast<Int64>(*opt) != v)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SYSTEM server_type.exclude_types[{}]: {}", i, v);
                server_type.exclude_types.insert(*opt);
            }
        }
        if (srv_obj->has("exclude_custom_names"))
        {
            auto arr = srv_obj->getArray("exclude_custom_names");
            if (!arr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'server_type.exclude_custom_names' is not a JSON array");
            for (unsigned int i = 0; i < arr->size(); ++i)
                server_type.exclude_custom_names.insert(arr->getElement<String>(i));
        }
    }
    cluster = r.getString("cluster");
}


}
