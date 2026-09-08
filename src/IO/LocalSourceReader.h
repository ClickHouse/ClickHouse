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
        : read_settings(std::move(read_settings_)), read_hint(read_hint_) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override;

    String name() const override { return "LocalSourceReader"; }

private:
    ReadSettings read_settings;
    /// The caller's read hint, as the legacy local path passes it: it is what
    /// `createReadBufferFromFileBase` sizes the buffer and keys the mmap / direct-IO
    /// thresholds on when it is present.
    std::optional<size_t> read_hint;
    LoggerPtr log = getLogger("LocalSourceReader");
};

}
