#pragma once

#include <IO/WriteBufferFromFileBase.h>

#include <functional>

namespace DB
{

/// Defers the inline-vs-blob decision until the file size is known: the first byte past
/// `max_inline_bytes` spills everything into `create_underlying`; otherwise `commit_inline`
/// receives the whole content at finalize and no blob is written.
class WriteBufferInlineOrBlob final : public WriteBufferFromFileBase
{
public:
    using CreateUnderlying = std::function<std::unique_ptr<WriteBufferFromFileBase>()>;
    using CommitInline = std::function<void(String content)>;

    WriteBufferInlineOrBlob(
        String file_name_,
        size_t max_inline_bytes_,
        CreateUnderlying create_underlying_,
        CommitInline commit_inline_,
        size_t buf_size);

    void preFinalize() override;
    void sync() override;
    std::string getFileName() const override { return file_name; }

private:
    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

    void spill();

    const String file_name;
    const size_t max_inline_bytes;
    const CreateUnderlying create_underlying;
    const CommitInline commit_inline;

    String accumulated;
    std::unique_ptr<WriteBufferFromFileBase> underlying;
};

}
