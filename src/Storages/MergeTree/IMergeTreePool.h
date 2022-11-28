#include <boost/noncopyable.hpp>
#include <IO/ReadBufferFromFileBase.h>
#include <Core/Block.h>

namespace DB
{

class MergeTreeReadTask;
using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

class IMergeTreeReadPool : private boost::noncopyable
{
public:
    virtual ~IMergeTreeReadPool() = default;

    virtual MergeTreeReadTaskPtr getTask(size_t min_marks_to_read, size_t thread, const Names & ordered_names) = 0;

    virtual void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) = 0;

    virtual Block getHeader() const = 0;
};

using MergeTreeReadPoolPtr = std::shared_ptr<IMergeTreeReadPool>;

}
