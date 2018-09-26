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

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** A stream of channel blocks from a shared vector of blocks
  */
class ChannelBlocksBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Acquires shared ownership of blocks pointers
    ChannelBlocksBlockInputStream(std::vector<std::shared_ptr<BlocksPtr>>& blocks_ptrs_)
        : blocks_ptrs(blocks_ptrs_), it(blocks_ptrs.begin()), end(blocks_ptrs.end()) {
            /// If there are any streams set blocks iterators to the first stream
            if (it != end)
            {
                blocks_it = (**it)->begin();
                blocks_end = (**it)->end();
            }
        }

    String getName() const override { return "Blocks"; }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();

        if (blocks_it == blocks_end)
        {
            ++it;
            if (it != end)
            {
                blocks_it = (**it)->begin();
                blocks_end = (**it)->end();
            }
            return readImpl();
        }

        Block res = *blocks_it;
        ++blocks_it;

        if (blocks_it == blocks_end)
            res.info.is_end_frame = true;

        return res;
    }

private:
    std::vector<std::shared_ptr<BlocksPtr>> blocks_ptrs;
    Blocks::iterator blocks_it;
    Blocks::iterator blocks_end;
    std::vector<std::shared_ptr<BlocksPtr>>::iterator it;
    const std::vector<std::shared_ptr<BlocksPtr>>::iterator end;
};

}
