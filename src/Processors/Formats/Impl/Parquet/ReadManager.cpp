#include "Read.h"

namespace DB::Parquet
{

ReadManager::ReadManager(ReadBuffer * reader_, const ReadOptions & options, SharedParsingThreadPoolPtr thread_pool_)
    : thread_pool(thread_pool_), reader(reader_, options, thread_pool_)
{
    reader.readFileMetaData();
}

ReadManager::~ReadManager()
{
    shutdown->shutdown();
}

}
