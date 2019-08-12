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

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** A stream of blocks from a shared vector of blocks
  */
class BlocksBlockInputStream : public IBlockInputStream
{
public:
    /// Acquires shared ownership of the blocks vector
    BlocksBlockInputStream(const std::shared_ptr<BlocksPtr> & blocks_ptr_, Block header_)
        : blocks(*blocks_ptr_), it((*blocks_ptr_)->begin()), end((*blocks_ptr_)->end()), header(std::move(header_)) {}

    String getName() const override { return "Blocks"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();

        Block res = *it;
        ++it;
        return res;
    }

private:
    BlocksPtr blocks;
    Blocks::iterator it;
    const Blocks::iterator end;
    Block header;
};

}
