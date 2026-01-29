#pragma once

#include <IO/WriteBufferFromVector.h>
#include <Common/StrictString.h>

namespace DB
{

class WriteBufferFromStrictString final : public WriteBufferFromVectorImpl<StrictString>
{
    using Base = WriteBufferFromVectorImpl;
public:
    explicit WriteBufferFromStrictString(StrictString & vector_)
        : Base(vector_)
    {
    }

    WriteBufferFromStrictString(StrictString & vector_, AppendModeTag tag_)
        : Base(vector_, tag_)
    {
    }
    ~WriteBufferFromStrictString() override;
};

}
