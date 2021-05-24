#pragma once

#include <exception>
#include <list>
#include <memory>

#include <Core/Block.h>
#include <Common/Cache/CompleteCache.h>
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

class SequentialBlockInputStream : public IBlockInputStream
{
    public:
    /// Acquires the ownership of the block list.
    SequentialBlockInputStream(BlockInputStreams && streams_)
        : streams(std::move(streams_)), it(streams.begin()), end(streams.end()) {}

    String getName() const override { return "BlockInputStreams"; }

protected:

    Block getHeader() const override 
    { 
        return streams.empty() ? Block() : (*streams.begin())->getHeader(); 
    }


    Block readImpl() override
    {
        Block res;
        while (it != end)
        {
            if ((res = (*it)->read()))
                return res;
            ++it;
        }
        return Block();
    }

private:
    BlockInputStreams streams;
    BlockInputStreams::iterator it;
    const BlockInputStreams::iterator end;
};

namespace ErrorCodes
{
    extern const int TOO_LARGE_DATASET;
}

class QueryCacheValue 
{
public:
    QueryCacheValue(
        BlocksList && blocks_, 
        size_t bytes_size_) : 
        blocks(std::move(blocks_)), 
        bytes_size(bytes_size_)
        {}
    
    const BlocksList& getBlocks() const { return blocks; }

    BlockInputStreamPtr getInputStream()
    { 
        return std::make_shared<BlocksListBlockInputStream>(blocks.begin(), blocks.end()); 
    }
    size_t getSize() const { return bytes_size; }

private:
    BlocksList blocks;
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


using QueryCacheBase = TTLLFUCache<UInt128, QueryCacheValue, std::hash<UInt128>, QueryCacheWeightFunction>;

// template <Class TCache>
class QueryCache : virtual public QueryCacheBase
{
public:

    explicit QueryCache(size_t max_size_in_bytes = 1)
        : QueryCacheBase(max_size_in_bytes) {}

    // using Key = typename QueryCacheBase::Key;
    // using Mapped = typename QueryCacheBase::Mapped;
    // using MappedPtr = std::shared_ptr<Mapped>;


    /// Calculate key from serialized query AST and offset.
    static UInt128 hash_str(const ASTPtr & select_query)
    {
        String serialized_ast = select_query->dumpTree();
        UInt128 key;

        SipHash hash;
        hash.update(serialized_ast.data(), serialized_ast.size() + 1);
        hash.get128(key);
        return key;
    }

    static UInt128 hash_tree(const ASTPtr & select_query)
    {
        String str_hash;
        {
            auto tree_hash = select_query->getTreeHash();
            str_hash = toString(tree_hash.first) + '_' + toString(tree_hash.second);
        }
        UInt128 key;
        SipHash hash;
        hash.update(str_hash.data(), str_hash.size() + 1);
        hash.get128(key);
        return key;
    }

    template <typename Load>
    BlockInputStreamPtr getOrSet(const Key & key, Load && load_func_, UInt64 cache_ttl = 2)
    {
        BlockInputStreamPtr stream;

        auto load_func = [this, &load_func_, &stream]() { 
            stream = load_func_();
            MappedPtr cache_value = this->streamHandler(stream);
            return cache_value; 
        };
        MappedPtr cache_value;

        auto result = QueryCacheBase::getOrSet(key, load_func, cache_ttl);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::QueryCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::QueryCacheMisses);

        if (!result.first)
            ProfileEvents::increment(ProfileEvents::QueryCacheInsertFails);

        return stream;
    }

    BlockInputStreamPtr get(const Key & key) 
    {
        auto result = QueryCacheBase::get(key);
        if (result)
        {
            ProfileEvents::increment(ProfileEvents::QueryCacheHits);
            return result->getInputStream();
        }
        ProfileEvents::increment(ProfileEvents::QueryCacheMisses);
        return BlockInputStreamPtr();
    }

    BlockInputStreamPtr trySet(const Key & key, BlockInputStreamPtr stream, UInt64 cache_ttl) 
    {
        BlockInputStreamPtr result;
        MappedPtr mapped;
        mapped = streamHandler(stream);
        if (mapped)
            QueryCacheBase::set(key, mapped, cache_ttl);
        else
            ProfileEvents::increment(ProfileEvents::QueryCacheInsertFails);
        return stream;

    }

private: 
    MappedPtr streamHandler(BlockInputStreamPtr & stream, size_t max_size_=(1u << 20))
    {
        Block block;
        BlocksList blocks;
        MappedPtr cache_value;
        size_t cur_size = 0;
        while ((block = stream->read()))
        {
            cur_size += block.bytes();
            blocks.push_back(std::move(block));
            if (cur_size > max_size_)
                break;
        }
        if (cur_size <= max_size_) 
        {
            cache_value = std::make_shared<QueryCacheValue>(std::move(blocks), cur_size);
            stream = cache_value->getInputStream();
        }
        else
        {
            BlockInputStreamPtr stored_blocks = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
            BlockInputStreamPtr remaining_blocks = stream;
            BlockInputStreams streams{stored_blocks, remaining_blocks};
            stream = std::make_shared<SequentialBlockInputStream>(std::move(streams));
        }
        return cache_value;
    }

    
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

}
