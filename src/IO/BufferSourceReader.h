#pragma once

#include <IO/ISourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

#include <functional>

namespace DB
{

class BufferSourceReader : public ISourceReader
{
public:
    using BufferFactory = std::function<std::unique_ptr<ReadBufferFromFileBase>(const StoredObject & object)>;

    explicit BufferSourceReader(BufferFactory factory_, String name_ = "BufferSource")
        : factory(std::move(factory_))
        , source_name(std::move(name_))
    {
    }

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        /// Factory is expected to honor external-buffer mode where its underlying
        /// API supports it; the executor drives reads via set()+next() either way.
        return factory(object);
    }

    String name() const override { return source_name; }

private:
    BufferFactory factory;
    String source_name;
    LoggerPtr log = getLogger("BufferSourceReader");
};

}
