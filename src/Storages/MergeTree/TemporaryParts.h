#pragma once

#include <boost/noncopyable.hpp>
#include <mutex>
#include <string>
#include <unordered_set>

namespace DB
{

/// Manages set of active temporary paths that should not be cleaned by background thread.
class TemporaryParts : private boost::noncopyable
{
private:
    /// To add const qualifier for contains()
    mutable std::mutex mutex;

    /// NOTE: It is pretty short, so use STL is fine.
    std::unordered_set<std::string> parts;

    void add(const std::string & basename);
    void remove(const std::string & basename);

    friend class MergeTreeData;
public:
    /// Returns true if passed part name is active.
    /// (is the destination for one of active mutation/merge).
    ///
    /// NOTE: that it accept basename (i.e. dirname), not the path,
    /// since later requires canonical form.
    bool contains(const std::string & basename) const;
};

}
