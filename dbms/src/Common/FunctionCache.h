#pragma once

#include <Core/Field.h>
#include <Core/Block.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Functions/IFunction.h>

#include <mutex>

namespace DB
{

class FunctionCache
{
public:
    FunctionCache(size_t columns_num_) : columns_num(columns_num_) {}

    void get(const FunctionBasePtr & func, size_t column_index, const Field & arg_field, Field & res_field);
    void addBlock(Block && block);

private:
    const size_t columns_num;
    std::vector<std::shared_ptr<Block>> blocks;

    struct Mapping
    {
        Block * block;
        size_t row;
    };

    struct FieldHash
    {
        size_t operator()(const Field & field) const
        {
            SipHash hash;
            applyVisitor(FieldVisitorHash(hash), field);
            return hash.get64();
        }
    };

    using Cache = std::unordered_map<Field, Mapping, FieldHash>;
    Cache cache;
};

using FunctionCachePtr = std::shared_ptr<FunctionCache>;

}
