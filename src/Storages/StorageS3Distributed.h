#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include "Client/Connection.h"
#include "Interpreters/Cluster.h"
#include "Storages/IStorage.h"
#include "Storages/StorageS3.h"

#include <memory>
#include <optional>
#include "ext/shared_ptr_helper.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class Context;

struct ClientAuthentificationBuilder
{
    String access_key_id;
    String secret_access_key;
    UInt64 max_connections;
};

using QueryId = std::string;
using Task = std::string;
using Tasks = std::vector<Task>;
using TasksIterator = Tasks::iterator;

class S3NextTaskResolver
{
public:
    S3NextTaskResolver(QueryId query_id, Tasks && all_tasks)
        : id(query_id)
        , tasks(all_tasks)
        , current(tasks.begin())
    {}

    std::string next()
    {
        auto it = current;
        ++current;
        return it == tasks.end() ? "" : *it;
    }

    std::string getId()
    {
        return id;
    }

private:
    QueryId id;
    Tasks tasks;
    TasksIterator current;
};

using S3NextTaskResolverPtr = std::shared_ptr<S3NextTaskResolver>;

class TaskSupervisor
{
public:
    using QueryId = std::string;

    TaskSupervisor() = default;

    static TaskSupervisor & instance()
    {
        static TaskSupervisor task_manager;
        return task_manager;
    }

    void registerNextTaskResolver(S3NextTaskResolverPtr resolver)
    {
        std::lock_guard lock(mutex);
        auto & target = dict[resolver->getId()];
        if (target)
            throw Exception(fmt::format("NextTaskResolver with name {} is already registered for query {}",
                target->getId(), resolver->getId()), ErrorCodes::LOGICAL_ERROR);
        target = std::move(resolver);
    }


    Task getNextTaskForId(const QueryId & id)
    {
        std::lock_guard lock(mutex);
        auto it = dict.find(id);
        if (it == dict.end())
            return "";
        auto answer = it->second->next();
        if (answer.empty())
            dict.erase(it); 
        return answer;
    }

private:
    using ResolverDict = std::unordered_map<QueryId, S3NextTaskResolverPtr>;
    ResolverDict dict;
    std::mutex mutex;
};

class StorageS3Distributed : public ext::shared_ptr_helper<StorageS3Distributed>, public IStorage 
{
    friend struct ext::shared_ptr_helper<StorageS3Distributed>;
public:
    std::string getName() const override { return "S3Distributed"; }

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;


protected:
    StorageS3Distributed(
        IAST::Hash tree_hash_,
        const String & address_hash_or_filename_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        String cluster_name_,
        const String & format_name_,
        UInt64 max_connections_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_,
        const String & compression_method_);

private:
    /// Connections from initiator to other nodes
    std::vector<std::shared_ptr<Connection>> connections;
    IAST::Hash tree_hash;
    String address_hash_or_filename;
    std::string cluster_name;
    ClusterPtr cluster;

    String format_name;
    String compression_method;
    ClientAuthentificationBuilder cli_builder;
};


}

#endif
