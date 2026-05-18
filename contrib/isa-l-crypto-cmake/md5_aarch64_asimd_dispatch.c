#include "md5_mb_internal.h"

void md5_ctx_mgr_init_asimd(ISAL_MD5_HASH_CTX_MGR * mgr);

ISAL_MD5_HASH_CTX * md5_ctx_mgr_submit_asimd(
    ISAL_MD5_HASH_CTX_MGR * mgr,
    ISAL_MD5_HASH_CTX * ctx,
    const void * buffer,
    uint32_t len,
    ISAL_HASH_CTX_FLAG flags);

ISAL_MD5_HASH_CTX * md5_ctx_mgr_flush_asimd(ISAL_MD5_HASH_CTX_MGR * mgr);

void _md5_ctx_mgr_init(ISAL_MD5_HASH_CTX_MGR * mgr)
{
    md5_ctx_mgr_init_asimd(mgr);
}

ISAL_MD5_HASH_CTX * _md5_ctx_mgr_submit(
    ISAL_MD5_HASH_CTX_MGR * mgr,
    ISAL_MD5_HASH_CTX * ctx,
    const void * buffer,
    uint32_t len,
    ISAL_HASH_CTX_FLAG flags)
{
    return md5_ctx_mgr_submit_asimd(mgr, ctx, buffer, len, flags);
}

ISAL_MD5_HASH_CTX * _md5_ctx_mgr_flush(ISAL_MD5_HASH_CTX_MGR * mgr)
{
    return md5_ctx_mgr_flush_asimd(mgr);
}
