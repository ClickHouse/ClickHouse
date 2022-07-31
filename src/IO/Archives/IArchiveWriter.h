#pragma once

#include <base/types.h>
#include <memory>


namespace DB
{
class WriteBufferFromFileBase;

/// Interface for writing an archive.
class IArchiveWriter : public std::enable_shared_from_this<IArchiveWriter>
{
public:
    /// Destructors finalizes writing the archive.
    virtual ~IArchiveWriter() = default;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) = 0;

    /// Returns true if there is an active instance of WriteBuffer returned by writeFile().
    /// This function should be used mostly for debugging purposes.
    virtual bool isWritingFile() const = 0;

    static constexpr const int kDefaultCompressionLevel = -1;

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    virtual void setCompression(int /* compression_method */, int /* compression_level */ = kDefaultCompressionLevel) {} /// NOLINT

    /// Sets password. If the password is not empty it will enable encryption in the archive.
    virtual void setPassword(const String & /* password */) {}
};

}
