#pragma once

#include <IO/WriteBufferFromVector.h>
#include <Common/TrackedString.h>

namespace DB
{

class WriteBufferFromTrackedString final : public WriteBufferFromVectorImpl<TrackedString>
{
    using Base = WriteBufferFromVectorImpl;
public:
    explicit WriteBufferFromTrackedString(TrackedString & vector_)
        : Base(vector_)
    {
    }

    WriteBufferFromTrackedString(TrackedString & vector_, AppendModeTag tag_)
        : Base(vector_, tag_)
    {
    }
    ~WriteBufferFromTrackedString() override;
};

}
