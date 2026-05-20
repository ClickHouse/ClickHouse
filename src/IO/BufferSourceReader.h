#pragma once

#include <IO/ISourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

#include <functional>

namespace DB
{

/// ISourceReader adapter for any source that produces a ReadBufferFromFileBase.
/// Used for BackupSource, CustomSource, and other non-standard sources.
class BufferSourceReader : public ISourceReader
{
public:
    using BufferFactory = std::function<std::unique_ptr<ReadBufferFromFileBase>(const StoredObject & object)>;

    explicit BufferSourceReader(BufferFactory factory_, String name_ = "BufferSource")
        : factory(std::move(factory_))
        , source_name(std::move(name_))
    {
    }

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object, bool /* use_external_buffer */) override
    {
        return factory(object);
    }

    String name() const override { return source_name; }

private:
    BufferFactory factory;
    String source_name;
    LoggerPtr log = getLogger("BufferSourceReader");
};

}
