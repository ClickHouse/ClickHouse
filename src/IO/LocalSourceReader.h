#pragma once

#include <IO/IFileBasedSourceReader.h>
#include <IO/ReadSettings.h>
#include <Common/Logger.h>

#include <optional>

namespace DB
{

/// Reads from local filesystem.
class LocalSourceReader : public IFileBasedSourceReader
{
public:
    explicit LocalSourceReader(ReadSettings read_settings_ = {}, std::optional<size_t> read_hint_ = {})
        : read_settings(std::move(read_settings_))
        , read_hint(read_hint_)
    {
    }

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override;

    String name() const override { return "LocalSourceReader"; }

private:
    ReadSettings read_settings;
    std::optional<size_t> read_hint;
    LoggerPtr log = getLogger("LocalSourceReader");
};

}
