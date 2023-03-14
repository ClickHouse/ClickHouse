#include <Interpreters/RewriteMapElementConstKeyToShardVisitor.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/FieldToDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/WeakHash.h>

namespace DB
{

namespace
{

UInt32 getWeakHash(const Field & field)
{
    auto type = applyVisitor(FieldToDataType(), field);
    auto tmp = type->createColumn();
    tmp->insert(field);

    WeakHash32 hash(1);
    tmp->updateWeakHash32(hash);
    return hash.getData()[0];
}

}

void RewriteMapElementConstKeyToShardData::visit(ASTFunction & function, ASTPtr & ast) const
{
    if (!storage_snapshot || !storage_snapshot->storage.supportsSubcolumns())
        return;

    if (function.name != "arrayElement")
        return;

    if (!function.arguments || function.arguments->children.size() != 2)
        return;

    const auto & arguments = function.arguments->children;
    const auto * identifier = arguments[0]->as<ASTIdentifier>();

    if (!identifier)
        return;

    const auto & columns = storage_snapshot->metadata->getColumns();
    const auto & name_in_storage = identifier->name();

    if (!columns.has(name_in_storage))
        return;

    const auto & column_type = columns.get(name_in_storage).type;
    const auto * type_map = typeid_cast<const DataTypeMap *>(column_type.get());

    if (!type_map || type_map->getNumShards() == 1)
        return;

    const auto * literal = arguments[1]->as<ASTLiteral>();
    if (!literal)
        return;

    UInt32 key_hash = getWeakHash(literal->value);
    UInt32 shard_num = key_hash % type_map->getNumShards();

    const auto & alias = function.tryGetAlias();
    auto subcolumn_name = ".shard" + toString(shard_num);

    auto first_arg = std::make_shared<ASTIdentifier>(name_in_storage + subcolumn_name);
    ast = makeASTFunction("arrayElement", first_arg, arguments[1]->clone());
    ast->setAlias(alias);
}

}
