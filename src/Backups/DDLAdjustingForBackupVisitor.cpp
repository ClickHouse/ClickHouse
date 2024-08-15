#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Parsers/formatAST.h>


namespace DB
{

namespace
{
    void visitStorageSystemTableEngine(ASTStorage &, const DDLAdjustingForBackupVisitor::Data & data)
    {
        /// Precondition: storage.engine && storage.engine->name.starts_with("System"))

        /// If this is a definition of a system table we'll remove columns and comment because they're redundant for backups.
        auto & create = data.create_query->as<ASTCreateQuery &>();
        create.reset(create.columns_list);
        create.reset(create.comment);
    }

    void visitStorageReplicatedTableEngine(ASTStorage & storage, const DDLAdjustingForBackupVisitor::Data & data)
    {
        /// Precondition: engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree")

        if (data.replicated_table_shared_id)
            *data.replicated_table_shared_id = StorageReplicatedMergeTree::tryGetTableSharedIDFromCreateQuery(*data.create_query, data.global_context);

        /// Before storing the metadata in a backup we have to find a zookeeper path in its definition and turn the table's UUID in there
        /// back into "{uuid}", and also we probably can remove the zookeeper path and replica name if they're default.
        /// So we're kind of reverting what we had done to the table's definition in registerStorageMergeTree.cpp before we created this table.
        auto & create = data.create_query->as<ASTCreateQuery &>();
        auto & engine = *storage.engine;

        auto * engine_args_ast = typeid_cast<ASTExpressionList *>(engine.arguments.get());
        if (!engine_args_ast)
            return;

        auto & engine_args = engine_args_ast->children;
        if (engine_args.size() < 2)
            return;

        auto * zookeeper_path_ast = typeid_cast<ASTLiteral *>(engine_args[0].get());
        auto * replica_name_ast = typeid_cast<ASTLiteral *>(engine_args[1].get());
        if (zookeeper_path_ast && (zookeeper_path_ast->value.getType() == Field::Types::String) &&
            replica_name_ast && (replica_name_ast->value.getType() == Field::Types::String))
        {
            String & zookeeper_path_arg = zookeeper_path_ast->value.get<String>();
            String & replica_name_arg = replica_name_ast->value.get<String>();
            if (create.uuid != UUIDHelpers::Nil)
            {
                String table_uuid_str = toString(create.uuid);
                if (size_t uuid_pos = zookeeper_path_arg.find(table_uuid_str); uuid_pos != String::npos)
                    zookeeper_path_arg.replace(uuid_pos, table_uuid_str.size(), "{uuid}");
            }
            const auto & config = data.global_context->getConfigRef();
            if ((zookeeper_path_arg == StorageReplicatedMergeTree::getDefaultZooKeeperPath(config))
                && (replica_name_arg == StorageReplicatedMergeTree::getDefaultReplicaName(config))
                && ((engine_args.size() == 2) || !engine_args[2]->as<ASTLiteral>()))
            {
                engine_args.erase(engine_args.begin(), engine_args.begin() + 2);
            }
        }
    }

    void visitStorage(ASTStorage & storage, const DDLAdjustingForBackupVisitor::Data & data)
    {
        if (!storage.engine)
            return;

        const String & engine_name = storage.engine->name;
        if (engine_name.starts_with("System"))
            visitStorageSystemTableEngine(storage, data);
        else if (engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree"))
            visitStorageReplicatedTableEngine(storage, data);
    }

    void visitCreateQuery(ASTCreateQuery & create, const DDLAdjustingForBackupVisitor::Data & data)
    {
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;

        if (create.storage)
            visitStorage(*create.storage, data);
    }
}


bool DDLAdjustingForBackupVisitor::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return false;
}

void DDLAdjustingForBackupVisitor::visit(ASTPtr ast, const Data & data)
{
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
}

void adjustCreateQueryForBackup(ASTPtr ast, const ContextPtr & global_context, std::optional<String> * replicated_table_shared_id)
{
    if (replicated_table_shared_id)
        *replicated_table_shared_id = {};

    DDLAdjustingForBackupVisitor::Data data{ast, global_context, replicated_table_shared_id};
    DDLAdjustingForBackupVisitor::Visitor{data}.visit(ast);
}

}
