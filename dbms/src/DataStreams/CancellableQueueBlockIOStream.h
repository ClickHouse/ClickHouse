/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <limits>

#include <Common/ConcurrentBoundedQueue.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{


/** Is both an InputStream and an OutputStream.
  * When writing, puts the blocks in the queue.
  * When reading, it takes them out of the queue.
  * A thread-safe queue is used.
  * If the queue is empty, the read is blocked.
  * If the queue is full, the write is blocked.
  *
  * Used to temporarily store the result somewhere, and later pass it further.
  * Also used for synchronization, when you need to make several sources from one
  *  - for single-pass execution of several queries at once.
  * It can also be used for parallelization: several threads put blocks in the queue, and one - takes out.
  */

class CancellableQueueBlockIOStream : public IProfilingBlockInputStream, public IBlockOutputStream
{
public:
    CancellableQueueBlockIOStream(size_t queue_size_ = std::numeric_limits<int>::max())
        : queue_size(queue_size_), queue(queue_size) {}

    String getName() const override { return "CancellableQueueBlockIOStream"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

    void write(const Block & block) override
    {
        queue.push(block);
    }

    void cancel() override
    {
        IProfilingBlockInputStream::cancel();
        queue.clear();
        queue.push(Block());
    }

protected:
    Block readImpl() override
    {
        Block res;
        queue.pop(res);
        return res;
    }

private:
    size_t queue_size;

    using Queue = ConcurrentBoundedQueue<Block>;
    Queue queue;
};

}
