/* $OpenBSD: wp_locl.h,v 1.3 2016/12/21 15:49:29 jsing Exp $ */

#include <openssl/whrlpool.h>

__BEGIN_HIDDEN_DECLS

void whirlpool_block(WHIRLPOOL_CTX *,const void *,size_t);

__END_HIDDEN_DECLS
