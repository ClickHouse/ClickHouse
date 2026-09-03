#pragma once

#include <boost/noncopyable.hpp>
#include <Core/Block.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>


namespace DB
{

/// The interface that determines how tasks for reading (MergeTreeReadTask)
/// are distributed among data parts with ranges.
class IMergeTreeReadPool : private boost::noncopyable
{
public:
    virtual ~IMergeTreeReadPool() = default;
    virtual String getName() const = 0;
    virtual Block getHeader() const = 0;

    /// Returns true if tasks are returned in the same order as the order of ranges passed to pool
    virtual bool preservesOrderOfRanges() const = 0;

    /// task_idx is an implementation defined identifier that helps
    /// to get required task. E.g. it may be number of thread in case of Default reading type or an index of a part in case of InOrder/InReverseOrder reading type.
    virtual MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) = 0;
    virtual void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) = 0;
};

using MergeTreeReadPoolPtr = std::shared_ptr<IMergeTreeReadPool>;

}
