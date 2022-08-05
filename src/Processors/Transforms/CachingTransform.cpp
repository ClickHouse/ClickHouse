#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
        LOG_FATAL(&Poco::Logger::get("CachingTransform::transform"), "chunk.num_cols ={}, .structure = {}", chunk.getNumColumns(), chunk.dumpStructure());
	holder.insertChunk(chunk.clone());
}

};
