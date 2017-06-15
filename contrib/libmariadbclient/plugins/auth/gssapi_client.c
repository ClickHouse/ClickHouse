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

#include <gssapi/gssapi.h>
#include <string.h>
#include <stdio.h>
#include <mysql/plugin_auth.h>
#include <ma_server_error.h>
#include <mysql.h>
#include "gssapi_errmsg.h"

extern void log_client_error(MYSQL *mysql,const char *fmt,...);


/* This sends the error to the client */
static void log_error(MYSQL *mysql, OM_uint32 major, OM_uint32 minor, const char *msg)
{
  if (GSS_ERROR(major))
  {
    char sysmsg[1024];
    gssapi_errmsg(major, minor, sysmsg, sizeof(sysmsg));
    log_client_error(mysql,
      "Client GSSAPI error (major %u, minor %u) : %s - %s",
       major, minor, msg, sysmsg);
  }
  else
  {
    log_client_error(mysql, "Client GSSAPI error : %s", msg);
  }
}

int auth_client(char *principal_name, char *mech __attribute__((unused)),
                MYSQL *mysql, MYSQL_PLUGIN_VIO *vio)
{
  gss_buffer_desc input= {0,0};
  int ret= CR_ERROR;
  OM_uint32 major= 0, minor= 0;
  gss_ctx_id_t ctxt= GSS_C_NO_CONTEXT;
  gss_name_t service_name= GSS_C_NO_NAME;

  if (principal_name && principal_name[0])
  {
    /* import principal from plain text */
    gss_buffer_desc principal_name_buf;
    principal_name_buf.length= strlen(principal_name);
    principal_name_buf.value= (void *) principal_name;
    major= gss_import_name(&minor, &principal_name_buf, GSS_C_NT_USER_NAME, &service_name);
    if (GSS_ERROR(major))
    {
      log_error(mysql, major, minor, "gss_import_name");
      return CR_ERROR;
    }
  }

  do
  {
    gss_buffer_desc output= {0,0};
    major= gss_init_sec_context(&minor, GSS_C_NO_CREDENTIAL, &ctxt, service_name,
                                GSS_C_NO_OID, 0, 0, GSS_C_NO_CHANNEL_BINDINGS,
                                &input, NULL, &output, NULL, NULL);
    if (output.length)
    {
      /* send credential */
      if(vio->write_packet(vio, (unsigned char *)output.value, output.length))
      {
        /* Server error packet contains detailed message. */
        ret= CR_OK_HANDSHAKE_COMPLETE;
        gss_release_buffer (&minor, &output);
        goto cleanup;
      }
    }
    gss_release_buffer (&minor, &output);

    if (GSS_ERROR(major))
    {
       log_error(mysql, major, minor,"gss_init_sec_context");
       goto cleanup;
    }

    if (major & GSS_S_CONTINUE_NEEDED)
    {
      int len= vio->read_packet(vio, (unsigned char **) &input.value);
      if (len <= 0)
      {
        /* Server error packet contains detailed message. */
        ret= CR_OK_HANDSHAKE_COMPLETE;
        goto cleanup;
      }
      input.length= len;
    }
  } while (major & GSS_S_CONTINUE_NEEDED);

  ret= CR_OK;

cleanup:
  if (service_name != GSS_C_NO_NAME)
    gss_release_name(&minor, &service_name);
  if (ctxt != GSS_C_NO_CONTEXT)
    gss_delete_sec_context(&minor, &ctxt, GSS_C_NO_BUFFER);

  return ret;
}
