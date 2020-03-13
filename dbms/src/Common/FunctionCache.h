#pragma once

#include <Core/Field.h>
#include <Core/Block.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Functions/IFunction.h>
#include <Common/HashTable/HashMap.h>

#include <mutex>

namespace DB
{

class FunctionCache
{
public:
    FunctionCache(size_t columns_num_) : columns_num(columns_num_) {}

    void get(const FunctionBasePtr & func, const Field & arg_field, Field & res_field);
    void addBlock(Block && block);
    void clear()
    {
        cache.clear();
        blocks.clear();
    }

private:
    const size_t columns_num;
    std::vector<std::shared_ptr<Block>> blocks;

    struct Mapping
    {
        Block * block;
        size_t column;
        size_t row;
    };

    using Cache = HashMap<UInt128, Mapping, UInt128TrivialHash>;
    Cache cache;
};

using FunctionCachePtr = std::shared_ptr<FunctionCache>;

}
