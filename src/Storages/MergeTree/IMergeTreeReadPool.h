#pragma once

#include <boost/noncopyable.hpp>
#include <Core/Block.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{
struct MergeTreeReadTask;
using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;


class IMergeTreeReadPool : private boost::noncopyable
{
public:
    virtual ~IMergeTreeReadPool() = default;

    virtual Block getHeader() const = 0;

    virtual MergeTreeReadTaskPtr getTask(size_t thread) = 0;

    virtual void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) = 0;
};

using MergeTreeReadPoolPtr = std::shared_ptr<IMergeTreeReadPool>;

}
