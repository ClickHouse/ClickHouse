#pragma once
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

#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

/** A stream of blocks from a shared vector of blocks
  */
class BlocksSource : public ISource
{
public:
    /// Acquires shared ownership of the blocks vector
    BlocksSource(BlocksPtr blocks_ptr_, Block header)
        : ISource(std::move(header))
        , blocks(blocks_ptr_), it(blocks_ptr_->begin()), end(blocks_ptr_->end()) {}

    String getName() const override { return "Blocks"; }

protected:
    Chunk generate() override
    {
        if (it == end)
            return {};

        Block res = *it;
        ++it;

        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = res.info.bucket_num;
        info->is_overflows = res.info.is_overflows;

        return Chunk(res.getColumns(), res.rows(), std::move(info));
    }

private:
    BlocksPtr blocks;
    Blocks::iterator it;
    const Blocks::iterator end;
};

}
