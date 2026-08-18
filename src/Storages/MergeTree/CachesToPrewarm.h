#pragma once

#include <memory>

namespace DB
{

class MarkCache;
using MarkCachePtr = std::shared_ptr<MarkCache>;

class PrimaryIndexCache;
using PrimaryIndexCachePtr = std::shared_ptr<PrimaryIndexCache>;

/// Which caches to prewarm with the data of a part being written. Computed once per
/// insert/merge/mutation from one settings snapshot; the writers and the move of the
/// index into the cache after commit use the same decision.
struct CachesToPrewarm
{
    MarkCachePtr mark_cache;
    MarkCachePtr index_mark_cache;
    PrimaryIndexCachePtr primary_index_cache;

    /// The cache in use, regardless of prewarm; when set, the index must not stay
    /// in the part after commit.
    PrimaryIndexCachePtr used_primary_index_cache;

    /// Nothing to prewarm; the cache pointer only selects whether to keep the index in memory.
    static CachesToPrewarm noPrewarm(PrimaryIndexCachePtr used_primary_index_cache_)
    {
        CachesToPrewarm res;
        res.used_primary_index_cache = std::move(used_primary_index_cache_);
        return res;
    }

    bool hasAny() const { return mark_cache || index_mark_cache || primary_index_cache; }

    /// Save marks in memory if prewarm is enabled to avoid re-reading marks file.
    bool saveMarksInCache() const { return mark_cache || index_mark_cache; }

    /// Save primary index in memory if cache is disabled or is enabled with prewarm
    /// to avoid re-reading primary index file. In the latter case the index is moved
    /// into the cache after the part is committed (see `IMergeTreeDataPart::moveIndexToCache`).
    bool savePrimaryIndexInMemory() const { return !used_primary_index_cache || primary_index_cache; }
};

}
