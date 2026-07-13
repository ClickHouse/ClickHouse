#pragma once

namespace DB
{

class StorageFactory;

void registerStorageElasticsearchQueue(StorageFactory & factory);

}
