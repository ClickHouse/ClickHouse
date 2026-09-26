#pragma once

#include <IO/WriteBufferFromVector.h>
#include <Common/StringWithMemoryTracking.h>

namespace DB
{

class WriteBufferFromStringWithMemoryTracking final : public WriteBufferFromVectorImpl<StringWithMemoryTracking>
{
    using Base = WriteBufferFromVectorImpl;
public:
    explicit WriteBufferFromStringWithMemoryTracking(StringWithMemoryTracking & vector_)
        : Base(vector_)
    {
    }

    WriteBufferFromStringWithMemoryTracking(StringWithMemoryTracking & vector_, AppendModeTag tag_)
        : Base(vector_, tag_)
    {
    }
    ~WriteBufferFromStringWithMemoryTracking() override;
};

}
