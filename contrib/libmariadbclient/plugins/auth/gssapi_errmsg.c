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

#ifdef  __FreeBSD__
#include <gssapi/gssapi.h>
#else
#include <gssapi.h>
#endif
#include <string.h>
void gssapi_errmsg(OM_uint32 major, OM_uint32 minor, char *buf, size_t size)
{
  OM_uint32 message_context;
  OM_uint32 status_code;
  OM_uint32 maj_status;
  OM_uint32 min_status;
  gss_buffer_desc status_string;
  char *p= buf;
  char *end= buf + size - 1;
  int types[] = {GSS_C_GSS_CODE,GSS_C_MECH_CODE};
  int i;
  for(i= 0; i < 2;i++)
  {
    message_context= 0;
    status_code= types[i] == GSS_C_GSS_CODE?major:minor;

    if(!status_code)
      continue;
    do
    {
      maj_status = gss_display_status(
        &min_status,
        status_code,
        types[i],
        GSS_C_NO_OID,
        &message_context,
        &status_string);

      if(maj_status)
        break;

      if(p + status_string.length + 2 < end)
      {
        memcpy(p,status_string.value, status_string.length);
        p += status_string.length;
        *p++ = '.';
        *p++ = ' ';
      }

      gss_release_buffer(&min_status, &status_string);
    }
    while (message_context != 0);
  }
  *p= 0;
}
