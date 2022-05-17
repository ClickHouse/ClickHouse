#include <Backups/replaceTableUUIDWithMacroInReplicatedTableDef.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

void replaceTableUUIDWithMacroInReplicatedTableDef(ASTCreateQuery & create_query, const UUID & table_uuid)
{
    if (create_query.getTable().empty() || !create_query.storage || !create_query.storage->engine || (table_uuid == UUIDHelpers::Nil))
        return;

    auto & engine = *(create_query.storage->engine);
    if (!engine.name.starts_with("Replicated") || !engine.arguments)
        return;

    auto * args = typeid_cast<ASTExpressionList *>(engine.arguments.get());

    size_t zookeeper_path_arg_pos = engine.name.starts_with("ReplicatedGraphite") ? 1 : 0;

    if (!args || (args->children.size() <= zookeeper_path_arg_pos))
        return;

    auto * zookeeper_path_arg = typeid_cast<ASTLiteral *>(args->children[zookeeper_path_arg_pos].get());
    if (!zookeeper_path_arg || (zookeeper_path_arg->value.getType() != Field::Types::String))
        return;

    String & zookeeper_path = zookeeper_path_arg->value.get<String>();

    String table_uuid_str = toString(table_uuid);
    if (size_t uuid_pos = zookeeper_path.find(table_uuid_str); uuid_pos != String::npos)
        zookeeper_path.replace(uuid_pos, table_uuid_str.size(), "{uuid}");
}

}
