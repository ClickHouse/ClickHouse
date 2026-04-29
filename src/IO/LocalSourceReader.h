#pragma once

#include <IO/ISourceReader.h>

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

    String name() const override { return "LocalSourceReader"; }
};

}
