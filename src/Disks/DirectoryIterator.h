#pragma once

#include <string>
#include <memory>

namespace DB
{
/**
 * Iterator of directory contents
 */
class IDirectoryIterator
{
public:
    /// Iterate to the next file.
    virtual void next() = 0;

    /// Return `true` if the iterator points to a valid element.
    virtual bool isValid() const = 0;

    /// Path to the file that the iterator currently points to.
    virtual std::string path() const = 0;

    /// Name of the file that the iterator currently points to.
    virtual std::string name() const = 0;

    virtual ~IDirectoryIterator() = default;
};

using DirectoryIteratorPtr = std::unique_ptr<IDirectoryIterator>;

}
