/* Copyright (c) 2015, Shuang Qiu, Robbie Harwood,
Vladislav Vaintroub & MariaDB Corporation

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

#define SECURITY_WIN32
#include <windows.h>
#include <sspi.h>
#include <SecExt.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <mysql/plugin_auth.h>
#include <mysql.h>
#include <ma_server_error.h>

#include "sspi_common.h"

extern void log_client_error(MYSQL *mysql, const char *fmt, ...);
static void log_error(MYSQL *mysql, SECURITY_STATUS err, const char *msg)
{
  if (err)
  {
    char buf[1024];
    sspi_errmsg(err, buf, sizeof(buf));
    log_client_error(mysql, "SSPI client error 0x%x - %s - %s", err, msg, buf);
  }
  else
  {
    log_client_error(mysql, "SSPI client error %s", msg);
  }
}


/** Client side authentication*/
int auth_client(char *principal_name, char *mech, MYSQL *mysql, MYSQL_PLUGIN_VIO *vio)
{

  int ret;
  CredHandle cred;
  CtxtHandle ctxt;
  ULONG attribs = 0;
  TimeStamp lifetime;
  SECURITY_STATUS sspi_err;

  SecBufferDesc inbuf_desc;
  SecBuffer     inbuf;
  SecBufferDesc outbuf_desc;
  SecBuffer     outbuf;
  PBYTE         out = NULL;

  ret= CR_ERROR;
  SecInvalidateHandle(&ctxt);
  SecInvalidateHandle(&cred);

  if (!mech || strcmp(mech, "Negotiate") != 0)
  {
    mech= "Kerberos";
  }

  sspi_err = AcquireCredentialsHandle(
    NULL,
    mech,
    SECPKG_CRED_OUTBOUND,
    NULL,
    NULL,
    NULL,
    NULL,
    &cred,
    &lifetime);

  if (SEC_ERROR(sspi_err))
  {
    log_error(mysql, sspi_err, "AcquireCredentialsHandle");
    return CR_ERROR;
  }

  out = (PBYTE)malloc(SSPI_MAX_TOKEN_SIZE);
  if (!out)
  {
    log_error(mysql, SEC_E_OK, "memory allocation error");
    goto cleanup;
  }

  /* Prepare buffers */
  inbuf_desc.ulVersion = SECBUFFER_VERSION;
  inbuf_desc.cBuffers = 1;
  inbuf_desc.pBuffers = &inbuf;
  inbuf.BufferType = SECBUFFER_TOKEN;
  inbuf.cbBuffer = 0;
  inbuf.pvBuffer = NULL;

  outbuf_desc.ulVersion = SECBUFFER_VERSION;
  outbuf_desc.cBuffers = 1;
  outbuf_desc.pBuffers = &outbuf;
  outbuf.BufferType = SECBUFFER_TOKEN;
  outbuf.pvBuffer = out;

  do
  {
    outbuf.cbBuffer= SSPI_MAX_TOKEN_SIZE;
    sspi_err= InitializeSecurityContext(
      &cred,
      SecIsValidHandle(&ctxt) ? &ctxt : NULL,
      principal_name,
      0,
      0,
      SECURITY_NATIVE_DREP,
      inbuf.cbBuffer ? &inbuf_desc : NULL,
      0,
      &ctxt,
      &outbuf_desc,
      &attribs,
      &lifetime);
    if (SEC_ERROR(sspi_err))
    {
      log_error(mysql, sspi_err, "InitializeSecurityContext");
      goto cleanup;
    }
    if (sspi_err != SEC_E_OK && sspi_err != SEC_I_CONTINUE_NEEDED)
    {
      log_error(mysql, sspi_err, "Unexpected response from InitializeSecurityContext");
      goto cleanup;
    }

    if (outbuf.cbBuffer)
    {
      /* send credential to server */
      if (vio->write_packet(vio, (unsigned char *)outbuf.pvBuffer, outbuf.cbBuffer))
      {
        /* Server error packet contains detailed message. */
        ret= CR_OK_HANDSHAKE_COMPLETE;
        goto cleanup;
      }
    }

    if (sspi_err == SEC_I_CONTINUE_NEEDED)
    {
      int len= vio->read_packet(vio, (unsigned char **)&inbuf.pvBuffer);
      if (len <= 0)
      {
        /* Server side error is in the last server packet. */
        ret= CR_OK_HANDSHAKE_COMPLETE;
        goto cleanup;
      }
      inbuf.cbBuffer= len;
    }
  } while (sspi_err == SEC_I_CONTINUE_NEEDED);

  ret= CR_OK;

cleanup:

  if (SecIsValidHandle(&ctxt))
    DeleteSecurityContext(&ctxt);
  if (SecIsValidHandle(&cred))
    FreeCredentialsHandle(&cred);
  free(out);
  return ret;
}
