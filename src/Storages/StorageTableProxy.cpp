#include <Storages/StorageTableProxy.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

void StorageTableProxy::applyMetadataChangesToCreateQueryForBackup(const ASTPtr & create_query) const
{
    StoragePtr storage = tryGetNested();
    if (!storage)
    {
        const auto * create = create_query->as<ASTCreateQuery>();
        const bool is_replicated = create && create->storage && create->storage->engine
            && create->storage->engine->name.starts_with("Replicated");
        if (!is_replicated)
            return;
        storage = getNested();
    }
    storage->applyMetadataChangesToCreateQueryForBackup(create_query);
}

}
