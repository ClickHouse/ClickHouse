/************************************************************************************
  Copyright (C) 2014 MariaDB Corporation Ab

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA

  Author: Georg Richter

 *************************************************************************************/
#ifndef _ma_schannel_h_
#define _ma_schannel_h_

#define SECURITY_WIN32
#include <ma_global.h>
#include <ma_sys.h>
#include <ma_common.h>
#include <ma_pvio.h>
#include <errmsg.h>


#include <wincrypt.h>
#include <wintrust.h>


#include <security.h>

#include <schnlsp.h>
#undef SECURITY_WIN32
#include <Windows.h>
#include <sspi.h>

#define SC_IO_BUFFER_SIZE 0x4000


#include <ma_pthread.h>

struct st_schannel {
  HCERTSTORE cert_store;
  CERT_CONTEXT *client_cert_ctx;
  CredHandle CredHdl;
  my_bool FreeCredHdl;
  PUCHAR IoBuffer;
  DWORD IoBufferSize;
  SecPkgContext_StreamSizes Sizes;
  CtxtHandle ctxt;
  MYSQL *mysql;

  /* Cached data from the last read/decrypt call.*/
  SecBuffer extraBuf; /* encrypted data read from server. */
  SecBuffer dataBuf;  /* decrypted but still unread data from server.*/

};

typedef struct st_schannel SC_CTX;

extern HCERTSTORE ca_CertStore, crl_CertStore;
extern my_bool ca_Check, crl_Check;

CERT_CONTEXT *ma_schannel_create_cert_context(MARIADB_PVIO *pvio, const char *pem_file);
SECURITY_STATUS ma_schannel_client_handshake(MARIADB_TLS *ctls);
SECURITY_STATUS ma_schannel_handshake_loop(MARIADB_PVIO *pvio, my_bool InitialRead, SecBuffer *pExtraData);
my_bool ma_schannel_load_private_key(MARIADB_PVIO *pvio, CERT_CONTEXT *ctx, char *key_file);
PCCRL_CONTEXT ma_schannel_create_crl_context(MARIADB_PVIO *pvio, const char *pem_file);
my_bool ma_schannel_verify_certs(SC_CTX *sctx);
ssize_t ma_schannel_write_encrypt(MARIADB_PVIO *pvio,
                                 uchar *WriteBuffer,
                                 size_t WriteBufferSize);
 SECURITY_STATUS ma_schannel_read_decrypt(MARIADB_PVIO *pvio,
                                 PCredHandle phCreds,
                                 CtxtHandle * phContext,
                                 DWORD *DecryptLength,
                                 uchar *ReadBuffer,
                                 DWORD ReadBufferSize);


#endif /* _ma_schannel_h_ */
