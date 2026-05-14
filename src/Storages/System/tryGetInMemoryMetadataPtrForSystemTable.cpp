#include <Storages/System/tryGetInMemoryMetadataPtrForSystemTable.h>

#include <Storages/IStorage.h>
#include <Common/Exception.h>

namespace DB
{

StorageMetadataPtr tryGetInMemoryMetadataPtrForSystemTable(const StoragePtr & storage, ContextPtr context)
{
    try
    {
        return storage->getInMemoryMetadataPtr(context, false);
    }
    catch (...)
    {
        /// A dangling `Alias` whose target was dropped throws `UNKNOWN_TABLE`.
        tryLogCurrentException("tryGetInMemoryMetadataPtrForSystemTable");
        return nullptr;
    }
}

}
