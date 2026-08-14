/*
 * Unlike its siblings in the other platform directories, this one is not the output of
 * util/mkbuildinf.pl: there is no OpenSSL `Configure` target for "whatever architecture this
 * is", so this is the hand-written equivalent for the portable no-asm build (OPENSSL_NO_ASM).
 *
 * crypto/cversion.c and crypto/info.c are the only consumers; they surface these strings
 * through OpenSSL_version(). Keeping them constant also keeps the build reproducible.
 *
 * Copyright 2014-2025 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License 2.0 (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */

#define PLATFORM "platform: generic-no-asm"
#define DATE "built on: unknown"

static const char compiler_flags[] = "compiler: unknown";
