#pragma once

#include <memory>
#include <zstd.h>

namespace DB
{

/// Holders for the ZSTD compression and decompression contexts.
/// Holding the context in a unique_ptr member (as opposed to a raw pointer freed in the destructor)
/// also frees it when a constructor throws after creating the context: the destructor
/// of a partially constructed object is not called, but destructors of its members are.

struct ZstdCCtxDeleter
{
    void operator()(ZSTD_CCtx * cctx) const { ZSTD_freeCCtx(cctx); }
};

struct ZstdDCtxDeleter
{
    void operator()(ZSTD_DCtx * dctx) const { ZSTD_freeDCtx(dctx); }
};

using ZstdCCtxPtr = std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter>;
using ZstdDCtxPtr = std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter>;

}
