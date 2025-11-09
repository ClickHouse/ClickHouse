#pragma once
#include <base/types.h>
#include <IO/WriteBufferFromString.h>
#include <Columns/ColumnString.h>
namespace DB
{

struct PromptTemplate
{
    static void render(WriteBufferFromOwnString & text, const String & prompt, const ColumnString::Chars & data, const ColumnString::Offsets & offsets, size_t offset, size_t size);
};

}
