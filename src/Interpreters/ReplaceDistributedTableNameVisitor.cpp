#include <Interpreters/ReplaceDistributedTableNameVisitor.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionRemote.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

std::optional<StorageID> ReplaceDistributedTableNameVisitor::Scope::getLocalTable(const StorageID & table_name)
{
    if (tables_map.contains(table_name))
        return tables_map.at(table_name);
    return {};
}

std::optional<String> ReplaceDistributedTableNameVisitor::Scope::getDBName(const String & table_name)
{
    auto match_table_name = [&table_name](const StorageID & table) { return table_name == table.table_name; };

    auto find_in_current_scope = [&](const Tables & tables_)
    {
        size_t count = std::count_if(tables_.begin(), tables_.end(), match_table_name);
        if (count > 1)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR, "duplicated table name {}, maybe you should use 'db.table' style identifier.", table_name);

        return std::find_if(tables_.begin(), tables_.end(), match_table_name);
    };

    auto find_table = find_in_current_scope(tables);
    if (find_table != tables.end())
        return find_table->database_name;

    /// Find in parent query scope.
    /// For correlated subqueries.
    find_table = find_in_current_scope(parent_scope->tables);
    if (find_table != parent_scope->tables.end())
        return find_table->database_name;

    /// For local table, table alias or something else like 'xx.yy'
    return {};
}

void ReplaceDistributedTableNameVisitor::Scope::clear()
{
    tables.clear();
    parent_scope = nullptr;
}

void ReplaceDistributedTableNameVisitor::visit(ASTPtr & ast)
{
    ScopePtr scope = std::make_shared<Scope>(Tables(), TablesMap(), nullptr);
    visitImpl(ast, scope);
}

void ReplaceDistributedTableNameVisitor::visitImpl(ASTPtr & ast, ScopePtr & scope)
{
    enter(ast, scope);
    visitChildren(ast, scope);
    leave(ast, scope);
}

void ReplaceDistributedTableNameVisitor::enter(ASTPtr & ast, ScopePtr & scope)
{
    if (auto select_query = ast->as<ASTSelectQuery>())
        enter(*select_query, scope);
    else if (auto ident = ast->as<ASTIdentifier>())
        enter(*ident, scope);
}

void ReplaceDistributedTableNameVisitor::leave(ASTPtr & ast, ScopePtr & scope)
{
    if (auto select_query = ast->as<ASTSelectQuery>())
        leave(*select_query, scope);
}

void ReplaceDistributedTableNameVisitor::enter(ASTSelectQuery & select_query, ScopePtr & scope)
{
    scope = std::make_shared<Scope>(Tables(), TablesMap(), scope);

    if (auto tables = select_query.tables())
    {
        auto tables_in_select = tables->as<ASTTablesInSelectQuery>();
        for (auto & table_ele_ast : tables_in_select->children)
        {
            if (auto table_ele = table_ele_ast->as<ASTTablesInSelectQueryElement>())
            {
                if (table_ele->table_expression)
                {
                    auto table_expr = table_ele->table_expression->as<ASTTableExpression>();

                    if (table_expr->database_and_table_name)
                    {
                        enter(*table_expr->database_and_table_name->as<ASTTableIdentifier>(), scope);
                    }
                    else if (table_expr->table_function)
                    {
                        auto local_table_ident = enter(*table_expr->table_function->as<ASTFunction>(), table_expr->table_function, scope);
                        if (local_table_ident)
                        {
                            table_expr->database_and_table_name = local_table_ident;
                            table_expr->table_function.reset();
                        }
                    }
                }
            }
        }
    }
}

void ReplaceDistributedTableNameVisitor::leave(ASTSelectQuery &, ScopePtr & scope)
{
    /// Finish select query, reset scope.
    scope = scope->parent_scope;
}

std::shared_ptr<ASTTableIdentifier>
ReplaceDistributedTableNameVisitor::enter(ASTFunction & table_function, ASTPtr & table_function_ast, ScopePtr & /*scope*/)
{
    has_table_function = true;

    if (table_function.name != "remote" || table_function.name != "remoteSecure" || table_function.name != "cluster" || table_function.name != "clusterAllReplicas") /// TODO cluster
        return {};

    TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_function_ast, context);
    StoragePtr storage = table_function_ptr->execute(table_function_ast, context, table_function_ptr->getName());

    auto * storage_distributed = typeid_cast<StorageDistributed *>(storage.get());

    auto database_name = storage_distributed->getRemoteDatabaseName();
    auto table_name = storage_distributed->getRemoteTableName();
    auto local_table_ident = std::make_shared<ASTTableIdentifier>(database_name, table_name);
    auto local_table = local_table_ident->getTableId();

    storages.emplace_back(local_table);
    clusters.emplace_back(storage_distributed->getCluster());

    if (auto sharding_key = storage_distributed->getShardingKey())
    {
        WriteBufferFromOwnString write_buffer;
        IAST::FormatSettings settings(write_buffer, true, false, true);
        sharding_key->format(settings);
        sharding_keys.emplace_back(write_buffer.str());
    }
    else
    {
        sharding_keys.emplace_back();
    }

    has_distributed_table = true;

    return local_table_ident;
}

void ReplaceDistributedTableNameVisitor::enter(ASTTableIdentifier & table_ident, ScopePtr & scope)
{
    if (!table_ident.getDatabase())
        table_ident.resetTable(context->getCurrentDatabase(), table_ident.shortName());

    StoragePtr table = DatabaseCatalog::instance().getTable(table_ident.getTableId(), context);

    if (auto * distributed_table = dynamic_cast<StorageDistributed *>(table.get()))
    {
        /// 1. Initialize scope

        /// For example self join
        if (std::find(scope->tables.begin(), scope->tables.end(), table_ident.getTableId()) != scope->tables.end())
            return;

        scope->tables.push_back(table_ident.getTableId());
        auto database_name = distributed_table->getRemoteDatabaseName();
        auto table_name = distributed_table->getRemoteTableName();

        auto local_table_ident = std::make_shared<ASTTableIdentifier>(database_name, table_name);
        scope->tables_map.emplace(table_ident.getTableId(), local_table_ident->getTableId());

        /// 2. Replace distributed table to local table.
        auto local_table = local_table_ident->getTableId();
        auto local_table_meta = DatabaseCatalog::instance().getTable(local_table, context); /// the local table must exists

        if (!local_table_meta->isMergeTree())
            has_non_merge_tree_table = true;

        table_ident.resetTable(database_name, table_name);

        /// 3. Collect useful info.
        if (std::find(storages.begin(), storages.end(), local_table) == storages.end())
        {
            storages.emplace_back(local_table);
            clusters.emplace_back(distributed_table->getCluster());

            if (auto sharding_key = distributed_table->getShardingKey())
            {
                WriteBufferFromOwnString write_buffer;
                IAST::FormatSettings settings(write_buffer, true, false, true);
                sharding_key->format(settings);
                sharding_keys.emplace_back(write_buffer.str());
            }
            else
            {
                sharding_keys.emplace_back();
            }

            has_distributed_table = true;
        }
    }
    else
    {
        has_local_table = true;
    }
}

/// Replacing distributed table name for identifier.
void ReplaceDistributedTableNameVisitor::enter(ASTIdentifier & ident, ScopePtr & scope)
{
    /// ASTTableIdentifier is a special ASTIdentifier.
    if (ident.as<ASTTableIdentifier>())
        return;

    if (ident.compound())
    {
        auto part_num = ident.name_parts.size();

        String & distributed_table_name = ident.name_parts[part_num - 2];
        std::optional<String> distributed_db_name;

        if (part_num == 3) /// ident: db.tbl.col
            distributed_db_name = ident.name_parts[part_num - 3];
        else if (part_num == 2) /// ident: tbl.col
            distributed_db_name = scope->getDBName(distributed_table_name);

        if (distributed_db_name)
        {
            if (auto local_table = scope->getLocalTable({*distributed_db_name, distributed_table_name}))
            {
                if (local_table->database_name == distributed_db_name && part_num == 2)
                    ident = ASTIdentifier({local_table->table_name, ident.shortName()});
                else
                    ident = ASTIdentifier({local_table->database_name, local_table->table_name, ident.shortName()});
            }
        }
    }
}

}
