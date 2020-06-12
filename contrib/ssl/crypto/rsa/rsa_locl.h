/* $OpenBSD: rsa_locl.h,v 1.4 2016/12/21 15:49:29 jsing Exp $ */

__BEGIN_HIDDEN_DECLS

extern int int_rsa_verify(int dtype, const unsigned char *m,
    unsigned int m_len, unsigned char *rm, size_t *prm_len,
    const unsigned char *sigbuf, size_t siglen, RSA *rsa);

__END_HIDDEN_DECLS
