#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>

#include <string_view>

namespace DB
{

class WriteBuffer;

class PuffinWriter
{
public:
    explicit PuffinWriter(WriteBuffer & out_);

    PuffinBlob addBlob(PuffinBlob blob, std::string_view bytes);
    size_t finalize();

private:
    void writeHeaderIfNeeded();

    WriteBuffer & out;
    VectorWithMemoryTracking<PuffinBlob> blobs;
    UInt64 written_bytes = 0;
    bool finalized = false;
};

}
