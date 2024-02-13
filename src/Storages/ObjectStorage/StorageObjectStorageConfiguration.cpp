#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>


namespace DB
{

void StorageObjectStorageConfiguration::initialize(
    StorageObjectStorageConfiguration & configuration,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration.fromNamedCollection(*named_collection);
    else
        configuration.fromAST(engine_args, local_context, with_table_structure);
}

bool StorageObjectStorageConfiguration::withWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return getPath().find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::isPathWithGlobs() const
{
    return getPath().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::getPathWithoutGlob() const
{
    return getPath().substr(0, getPath().find_first_of("*?{"));
}

}
