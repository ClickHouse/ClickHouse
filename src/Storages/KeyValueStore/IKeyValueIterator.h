#pragma once

#include <base/types.h>
#include <memory>
#include <string>

namespace DB
{

/**
 * Abstract iterator interface for iterating over Key-Value store entries.
 * This provides a forward-only iterator abstraction that can be implemented
 * by different KV store backends (RocksDB, LevelDB, etc.)
 */
class IKeyValueIterator
{
public:
    virtual ~IKeyValueIterator() = default;

    /// Check if iterator is valid (points to a valid key-value pair)
    virtual bool valid() const = 0;

    /// Move to the next entry
    virtual void next() = 0;

    /// Move to the first entry (seek to start)
    virtual void seekToFirst() = 0;

    /// Move to the first entry with key >= target
    virtual void seek(const String & target) = 0;

    /// Get current key
    virtual String key() const = 0;

    /// Get current value
    virtual String value() const = 0;

    /// Get current status (for error checking)
    virtual bool status() const = 0;  // true if ok, false if error
};

using IKeyValueIteratorPtr = std::unique_ptr<IKeyValueIterator>;

}

