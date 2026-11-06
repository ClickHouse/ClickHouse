#pragma once

#include <functional>
#include <memory>
#include <string>

namespace DB
{

struct FsNode;
struct FsDirectoryEntry;

/// Subdirectories of a directory node, keyed by name.
class FsDirectoryEntries
{
public:
    std::shared_ptr<FsNode> findChild(const std::string & name) const;
    void putChild(const std::string & name, const std::shared_ptr<FsNode> & child);
    void removeChild(const std::string & name);
    bool isEmpty() const;
    void forEachChild(const std::function<void(const std::string & name, const std::shared_ptr<FsNode> & child)> & callback) const;

private:
    std::shared_ptr<const FsDirectoryEntry> root;
};

}
