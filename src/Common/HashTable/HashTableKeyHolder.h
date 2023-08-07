#pragma once

#include <base/StringRef.h>

#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/Exception.h>

/**
  * In some aggregation scenarios, when adding a key to the hash table, we
  * start with a temporary key object, and if it turns out to be a new key,
  * we must make it persistent (e.g. copy to an Arena) and use the resulting
  * persistent object as hash table key. This happens only for StringRef keys,
  * because other key types are stored by value, but StringRef is a pointer-like
  * type: the actual data are stored elsewhere. Even for StringRef, we don't
  * make a persistent copy of the key in each of the following cases:
  * 1) the aggregation method doesn't use temporary keys, so they're persistent
  *    from the start;
  * 2) the key is already present in the hash table;
  * 3) that particular key is stored by value, e.g. a short StringRef key in
  *    StringHashMap.
  *
  * In the past, the caller was responsible for making the key persistent after
  * in was inserted. emplace() returned whether the key is new or not, so the
  * caller only stored new keys (this is case (2) from the above list). However,
  * now we are adding a compound hash table for StringRef keys, so case (3)
  * appears. The decision about persistence now depends on some properties of
  * the key, and the logic of this decision is tied to the particular hash table
  * implementation. This means that the hash table user now doesn't have enough
  * data and logic to make this decision by itself.
  *
  * To support these new requirements, we now manage key persistence by passing
  * a special key holder to emplace(), which has the functions to make the key
  * persistent or to discard it. emplace() then calls these functions at the
  * appropriate moments.
  *
  * This approach has the following benefits:
  * - no extra runtime branches in the caller to make the key persistent.
  * - no additional data is stored in the hash table itself, which is important
  *   when it's used in aggregate function states.
  * - no overhead when the key memory management isn't needed: we just pass the
  *   bare key without any wrapper to emplace(), and the default callbacks do
  *   nothing.
  *
  * This file defines the default key persistence functions, as well as two
  * different key holders and corresponding functions for storing StringRef
  * keys to Arena.
  */

/**
  * Returns the key. Can return the temporary key initially.
  * After the call to keyHolderPersistKey(), must return the persistent key.
  */
template <typename Key>
inline Key & ALWAYS_INLINE keyHolderGetKey(Key && key) { return key; }

/**
  * Make the key persistent. keyHolderGetKey() must return the persistent key
  * after this call.
  */
template <typename Key>
inline void ALWAYS_INLINE keyHolderPersistKey(Key &&) {}

/**
  * Discard the key. Calling keyHolderGetKey() is ill-defined after this.
  */
template <typename Key>
inline void ALWAYS_INLINE keyHolderDiscardKey(Key &&) {}

namespace DB
{

/**
  * ArenaKeyHolder is a key holder for hash tables that serializes a StringRef
  * key to an Arena.
  */
struct ArenaKeyHolder
{
    StringRef key;
    Arena & pool;

};

}

inline StringRef & ALWAYS_INLINE keyHolderGetKey(DB::ArenaKeyHolder & holder)
{
    return holder.key;
}

inline void ALWAYS_INLINE keyHolderPersistKey(DB::ArenaKeyHolder & holder)
{
    // Normally, our hash table shouldn't ask to persist a zero key,
    // but it can happened in the case of clearable hash table (ClearableHashSet, for example).
    // The clearable hash table doesn't use zero storage and
    // distinguishes empty keys by using cell version, not the value itself.
    // So, when an empty StringRef is inserted in ClearableHashSet we'll get here key of zero size.
    // assert(holder.key.size > 0);
    holder.key.data = holder.pool.insert(holder.key.data, holder.key.size);
}

inline void ALWAYS_INLINE keyHolderDiscardKey(DB::ArenaKeyHolder &)
{
}

namespace DB
{

/** SerializedKeyHolder is a key holder for a StringRef key that is already
  * serialized to an Arena. The key must be the last allocation in this Arena,
  * and is discarded by rolling back the allocation.
  */
struct SerializedKeyHolder
{
    StringRef key;
    Arena & pool;
};

}

inline StringRef & ALWAYS_INLINE keyHolderGetKey(DB::SerializedKeyHolder & holder)
{
    return holder.key;
}

inline void ALWAYS_INLINE keyHolderPersistKey(DB::SerializedKeyHolder &)
{
}

inline void ALWAYS_INLINE keyHolderDiscardKey(DB::SerializedKeyHolder & holder)
{
    [[maybe_unused]] void * new_head = holder.pool.rollback(holder.key.size);
    assert(new_head == holder.key.data);
    holder.key.data = nullptr;
    holder.key.size = 0;
}
namespace DB
{
namespace ColumnsHashing
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
class HashMethodContext;
}
struct AdaptiveKeysHolder
{
    /// State is shared between all AdaptiveKeysHolder instances.
    /// Different hash_mode will have different behavior
    struct State
    {
        /// There are two modes.
        /// - VALUE_ID. For low cardinality keys. We allocate unique value_ids for each grouping keys.
        ///   This could avoid a lot of memory copying and is more efficient. This is mode is used
        ///   at the first.
        /// - HASH. It's like SerializedKeyHolder. After inserting some keys into the hash table, if
        ///   we found that the hash table size is to large, we will switch to this mode.
        enum HashMode
        {
            VALUE_ID = 0,
            HASH,
        };
        HashMode hash_mode = VALUE_ID;
        std::shared_ptr<Arena> pool;
    };

    /// be careful with all fields, their default value must be zero bits.
    /// since hash table allocs cell buffer without calling cell constructor.
    UInt64 value_id = 0;
    StringRef serialized_keys;
    State * state = nullptr;
};
}

inline bool ALWAYS_INLINE operator==(const DB::AdaptiveKeysHolder &a, const DB::AdaptiveKeysHolder &b)
{
    if (a.state->hash_mode != b.state->hash_mode)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "hash_mode is not equal");
    }
    /// a and b may come from different aggregate variants during the merging phase, in this case
    /// we cannot compare the value_ids.
    if (a.state->hash_mode == DB::AdaptiveKeysHolder::State::VALUE_ID && a.state == b.state)
    {
        return a.value_id == b.value_id;
    }
    return a.serialized_keys == b.serialized_keys;
}

inline DB::AdaptiveKeysHolder & ALWAYS_INLINE keyHolderGetKey(DB::AdaptiveKeysHolder & holder)
{
    return holder;
}

inline void ALWAYS_INLINE keyHolderPersistKey(DB::AdaptiveKeysHolder &)
{
}

inline void ALWAYS_INLINE keyHolderDiscardKey(DB::AdaptiveKeysHolder & holder)
{
    if (holder.state->hash_mode != DB::AdaptiveKeysHolder::State::VALUE_ID)
    {
        [[maybe_unused]] void * new_head = holder.state->pool->rollback(holder.serialized_keys.size);
        assert(new_head == holder.serialized_keys.data);
        holder.serialized_keys.data = nullptr;
        holder.serialized_keys.size = 0;
    }
}

template<>
struct DefaultHash<DB::AdaptiveKeysHolder>
{
    inline size_t operator()(const DB::AdaptiveKeysHolder & key) const
    {
        return ::DefaultHash<StringRef>()(key.serialized_keys);
    }
};
