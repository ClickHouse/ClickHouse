#pragma once

#include <IO/ISourceReader.h>
#include <IO/ReadSettings.h>
#include <Common/Logger.h>

namespace DB
{

/// Reads from local filesystem.
class LocalSourceReader : public ISourceReader
{
public:
    explicit LocalSourceReader(ReadSettings read_settings_ = {})
        : read_settings(std::move(read_settings_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override;

    String name() const override { return "LocalSourceReader"; }

private:
    ReadSettings read_settings;
    LoggerPtr log = getLogger("LocalSourceReader");
};

}
