#pragma once

#include <IO/ISourceReader.h>
#include <Common/Logger.h>

namespace DB
{

/// Reads from local filesystem using pread.
class LocalSourceReader : public ISourceReader
{
public:
    size_t read(
        const StoredObject & object,
        size_t offset, size_t size,
        char * buffer) override;

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override;

    String name() const override { return "LocalSourceReader"; }

private:
    LoggerPtr log = getLogger("LocalSourceReader");
};

}
