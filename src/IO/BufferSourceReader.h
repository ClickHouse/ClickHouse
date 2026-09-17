#pragma once

#include <IO/IFileBasedSourceReader.h>
#include <IO/ReadBufferFromFileBase.h>

#include <functional>

namespace DB
{

/// `IFileBasedSourceReader` over a caller-provided buffer factory.
class BufferSourceReader : public IFileBasedSourceReader
{
public:
    using BufferFactory = std::function<std::unique_ptr<ReadBufferFromFileBase>(const StoredObject & object)>;

    explicit BufferSourceReader(BufferFactory factory_, String name_ = "BufferSource");

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override;

    String name() const override { return source_name; }

private:
    BufferFactory factory;
    String source_name;
};

}
