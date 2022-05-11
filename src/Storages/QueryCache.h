#pragma once

#include <memory>

#include <Common/LRUCache.h>


namespace DB
{
using Data = std::pair<Block, Chunks>;

struct CacheKey
{
    bool operator==(const CacheKey & other) const
    {
        return ast->getTreeHash() == other.ast->getTreeHash() && header == other.header
            //               && settingsSet(settings) == settingsSet(other.settings)
            //               && username == other.username
            ;
    }

    ASTPtr ast;
    Block header;
    const Settings & settings;
    const std::optional<String> & username;

    //    static std::set<String> settingsSet(const Settings & settings) {
    //        std::set<String> res;
    //        for (const auto & s : settings.all()) {
    //            res.insert(s.getValueString());
    //        }
    //        return res;
    //    }
};

struct CacheKeyHasher
{
    size_t operator()(const CacheKey & k) const
    {
        auto ast_info = k.ast->getTreeHash();
        auto header_info = k.header.getNamesAndTypesList().toString();
        //        auto settings_info = settingsHash(k.settings);
        //        auto username_info = std::hash<std::optional<String>>{}(k.username);

        return ast_info.first + ast_info.second * 9273 + std::hash<String>{}(header_info)*9273 * 9273
            //            + settings_info * 9273 * 9273 * 9273
            //              + username_info * 9273 * 9273 * 9273 * 9273
            ;
    }
    //private:
    //    static size_t settingsHash(const Settings & settings) {
    //        size_t hash = 0;
    //        size_t coefficient = 1;
    //        for (const auto & s : settings) {
    //            hash += std::hash<String>{}(s.getValueString()) * coefficient;
    //            coefficient *= 53;
    //        }
    //        return hash;
    //    }
};

struct QueryWeightFunction
{
    size_t operator()(const Data & data) const
    {
        const Block & block = data.first;
        const Chunks & chunks = data.second;

        size_t res = 0;
        for (const auto & chunk : chunks)
        {
            res += chunk.bytes();
        }
        res += block.bytes();
        return res;
    }
};


class QueryCache : public LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>
{
private:
    using Base = LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>;

public:
    QueryCache(size_t cache_size_in_bytes, size_t cache_size_in_num_entries) : Base(cache_size_in_bytes, cache_size_in_num_entries) { }
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

}
