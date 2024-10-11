#pragma once

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <vector>
#include <memory>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/// Interface for accessing temporary files by some logical name (or id).
/// While building query pipeline processors can lookup temporary files by some id and use them for writing and/or reading temporary data
/// without knowing what exactly is behind the name: local file, memory buffer, object in cloud storage, etc.
struct ITemporaryFileLookup : boost::noncopyable
{
    virtual ~ITemporaryFileLookup() = default;

    /// Give the caller a temporary write buffer, but don't give away the ownership.
    virtual WriteBuffer & getTemporaryFileForWriting(const String & file_id) = 0;

    /// Give the caller a temporary read buffer, it exclusively belongs to the caller.
    /// Other callers can get their own read buffer for the same temporary file.
    virtual std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & file_id) = 0;
};

using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;

}
