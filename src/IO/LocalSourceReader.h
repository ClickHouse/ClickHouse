#pragma once

#include <IO/ISourceReader.h>
#include <Common/Logger.h>

namespace DB
{

/// Reads from local filesystem.
class LocalSourceReader : public ISourceReader
{
public:
    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object, bool use_external_buffer) override;

    String name() const override { return "LocalSourceReader"; }

private:
    LoggerPtr log = getLogger("LocalSourceReader");
};

}
