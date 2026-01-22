#pragma once

#include <Core/Types.h>
#include <base/types.h>
#include <memory>
#include <vector>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

enum class ManifestStatus : int32_t
{
    OK = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
    Busy = 6,
    TimedOut = 7,
};

class IManifestIterator
{
public:
    virtual ~IManifestIterator() = default;

    virtual void seek(const String & key) = 0;
    virtual void next() = 0;
    virtual bool valid() const = 0;
    virtual String key() const = 0;
    virtual String value() const = 0;
};

using ManifestIteratorPtr = std::unique_ptr<IManifestIterator>;

class IManifestStorage
{
public:
    virtual ~IManifestStorage() = default;

    virtual ManifestStatus put(const String & key, const String & value) = 0;
    virtual ManifestStatus get(const String & key, String & value) const = 0;
    virtual ManifestStatus del(const String & key) = 0;
    virtual ManifestStatus delRange(const String & start_key, const String & end_key) = 0;
    virtual ManifestStatus batchWrite(
        const std::vector<String> & keys_to_delete,
        const std::vector<std::pair<String, String>> & keys_to_put) = 0;
    virtual void getByPrefix(
        const String & prefix,
        std::vector<String> & keys,
        std::vector<String> & values) const = 0;
    virtual void getValuesByPrefix(
        const String & prefix,
        std::vector<String> & values) const = 0;
    virtual void getValuesByRange(
        const String & begin_key,
        const String & end_key,
        std::vector<String> & values) const = 0;
    virtual ManifestIteratorPtr createIterator() const = 0;
    virtual uint64_t getEstimateNumKeys() const = 0;
    virtual void flush() = 0;
    virtual void shutdown() = 0;
    virtual void drop() = 0;
};

using ManifestStoragePtr = std::shared_ptr<IManifestStorage>;

inline void throwIfNotOK(ManifestStatus status, const String & operation, const String & key = "")
{
    if (status != ManifestStatus::OK)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Failed to {} '{}', status: {}",
                        operation,
                        key.empty() ? "manifest" : key,
                        static_cast<int>(status));
    }
}

inline void throwIfNotOKOrNotFound(ManifestStatus status, const String & operation, const String & key = "")
{
    if (status != ManifestStatus::OK && status != ManifestStatus::NotFound)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Failed to {} '{}', status: {}",
                        operation,
                        key.empty() ? "manifest" : key,
                        static_cast<int>(status));
    }
}

}
