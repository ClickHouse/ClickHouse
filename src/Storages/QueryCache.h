#pragma once

#include <exception>
#include <list>
#include <memory>

#include <Core/Block.h>
#include <Common/LRUCache.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSelectQuery.h>

namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
    extern const Event QueryCacheInsertSuccess;
    extern const Event QueryCacheInsertFails;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_DATASET;
}

class QueryCacheValue 
{
public:
    QueryCacheValue(BlocksList && blocks_, size_t bytes_size_) : blocks(std::move(blocks_)), bytes_size(bytes_size_) {}
    const BlocksList& getBlocks() const { return blocks; }
    BlockInputStreamPtr getInputStream() 
    { 
        return std::make_shared<BlocksListBlockInputStream>(blocks.begin(), blocks.end()); 
    }
    // UInt32 getExpiresMs() const { return expires_ms; }
    size_t getSize() const { return bytes_size; }

private:
    BlocksList blocks;
    // UInt32 accessed_ms;
    // UInt32 expires_ms;
    size_t bytes_size;
};


/// Estimate of number of bytes in cache for marks.
struct QueryCacheWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t QUERY_CACHE_OVERHEAD = 128;

    size_t operator()(const QueryCacheValue & query_cache_mapped) const
    {
        size_t size_in_bytes = query_cache_mapped.getSize();
        return size_in_bytes + QUERY_CACHE_OVERHEAD;
    }
};


class QueryCache : public LRUCache<UInt128, QueryCacheValue, UInt128TrivialHash, QueryCacheWeightFunction>
{
private:
    using Base = LRUCache<UInt128, QueryCacheValue, UInt128TrivialHash, QueryCacheWeightFunction>;
    // size_t max_size_in_bytes_item_;



public:
    QueryCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from serialized query AST and offset.
    static UInt128 hash(const ASTPtr & select_query)
    {
        String serialized_ast = select_query->dumpTree();
        UInt128 key;

        SipHash hash;
        hash.update(serialized_ast.data(), serialized_ast.size() + 1);
        hash.get128(key.low, key.high);

        return key;
    }


    BlockInputStreamPtr getOrSet(const Key & key, BlockInputStreamPtr stream)
    //TODO
    {
        auto load_func = [this, &stream]() { 
            auto cache_value = this->streamHandler(stream);
            if (!cache_value)
                throw Exception("Too large dataset", ErrorCodes::TOO_LARGE_DATASET);
            return cache_value; 
        };
        MappedPtr cache_value;
        try
        {
            auto result = Base::getOrSet(key, load_func);
            if (result.second)
                ProfileEvents::increment(ProfileEvents::QueryCacheHits);
            else
                ProfileEvents::increment(ProfileEvents::QueryCacheMisses);
            cache_value = result.first;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::TOO_LARGE_DATASET)
            {
                ProfileEvents::increment(ProfileEvents::QueryCacheInsertFails);
                return BlockInputStreamPtr();
            }
            throw e;
        }

        return cache_value->getInputStream();
    }

    BlockInputStreamPtr get(const Key & key) 
    {
        auto result = Base::get(key);
        if (result)
        {
            ProfileEvents::increment(ProfileEvents::QueryCacheHits);
            return result->getInputStream();
        }
        ProfileEvents::increment(ProfileEvents::QueryCacheMisses);
        return BlockInputStreamPtr();
    }

    BlockInputStreamPtr trySet(const Key & key, BlockInputStreamPtr stream) 
    {
        BlockInputStreamPtr result;
        MappedPtr mapped;

        mapped = streamHandler(stream);
        if (mapped)
            Base::set(key, mapped);
        else
            ProfileEvents::increment(ProfileEvents::QueryCacheInsertFails);
        return stream;

    }

private: 
    MappedPtr streamHandler(BlockInputStreamPtr & stream, size_t max_size=(1u << 20))
    {
        Block block;
        BlocksList blocks;
        MappedPtr cache_value;
        size_t cur_size = 0;
        while ((block = stream->read()))
        {
            cur_size += block.bytes();
            blocks.push_back(std::move(block));
            if (cur_size > max_size)
                break;
        }
        if (cur_size <= max_size) 
        {
            cache_value = std::make_shared<QueryCacheValue>(std::move(blocks), cur_size);
            stream = cache_value->getInputStream();
        }
        else
        {
            BlockInputStreamPtr stored_blocks = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
            BlockInputStreamPtr remaining_blocks = stream;
            BlockInputStreams streams{stored_blocks, remaining_blocks};
            stream = std::make_shared<UnionBlockInputStream>(streams, BlockInputStreamPtr(), 1);
        }
        return cache_value;
    }
};

using QueryCachePtr = std::shared_ptr<QueryCache>;
}
