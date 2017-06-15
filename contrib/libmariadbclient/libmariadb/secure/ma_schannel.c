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
#include "ma_schannel.h"
#include <assert.h>

#define SC_IO_BUFFER_SIZE 0x4000
#define MAX_SSL_ERR_LEN 100

#define SCHANNEL_PAYLOAD(A) (A).cbMaximumMessage + (A).cbHeader + (A).cbTrailer
void ma_schannel_set_win_error(MARIADB_PVIO *pvio);

/* {{{ void ma_schannel_set_sec_error */
void ma_schannel_set_sec_error(MARIADB_PVIO *pvio, DWORD ErrorNo)
{
  MYSQL *mysql= pvio->mysql;
  switch(ErrorNo) {
  case SEC_E_ILLEGAL_MESSAGE:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: The message received was unexpected or badly formatted");
    break;
  case SEC_E_UNTRUSTED_ROOT:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Untrusted root certificate");
    break;
  case SEC_E_BUFFER_TOO_SMALL:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Buffer too small");
    break;
  case SEC_E_CRYPTO_SYSTEM_INVALID:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Cipher is not supported");
    break;
  case SEC_E_INSUFFICIENT_MEMORY:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Out of memory");
    break;
  case SEC_E_OUT_OF_SEQUENCE:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Invalid message sequence");
    break;
  case SEC_E_DECRYPT_FAILURE:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: An error occured during decrypting data");
    break;
  case SEC_I_INCOMPLETE_CREDENTIALS:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Incomplete credentials");
    break;
  case SEC_E_ENCRYPT_FAILURE:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: An error occured during encrypting data");
    break;
  case SEC_I_CONTEXT_EXPIRED:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Context expired ");
    break;
  case SEC_E_ALGORITHM_MISMATCH:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: no cipher match");
    break;
  case SEC_E_NO_CREDENTIALS:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: no credentials");
    break;
  case SEC_E_OK:
    break;
  case SEC_E_INTERNAL_ERROR:
    if (GetLastError())
      ma_schannel_set_win_error(pvio);
    else
      pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "The Local Security Authority cannot be contacted");
    break;
  default:
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Unknown SSL error (0x%x)", ErrorNo);
  }
}
/* }}} */

/* {{{ void ma_schnnel_set_win_error */
void ma_schannel_set_win_error(MARIADB_PVIO *pvio)
{
  ulong ssl_errno= GetLastError();
  char *ssl_error_reason= NULL;
  char *p;
  char buffer[256];
  if (!ssl_errno)
  {
    pvio->set_error(pvio->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Unknown SSL error");
    return;
  }
  /* todo: obtain error messge */
  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                NULL, ssl_errno, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                (LPTSTR) &ssl_error_reason, 0, NULL );
  for (p = ssl_error_reason; *p; p++)
    if (*p == '\n' || *p == '\r')
      *p = 0;
  snprintf(buffer, sizeof(buffer), "SSL connection error: %s",ssl_error_reason);
  pvio->set_error(pvio->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, buffer);
  if (ssl_error_reason)
    LocalFree(ssl_error_reason);
  return;
}
/* }}} */

/* {{{ LPBYTE ma_schannel_load_pem(const char *PemFileName, DWORD *buffer_len) */
/*
   Load a pem or clr file and convert it to a binary DER object

   SYNOPSIS
     ma_schannel_load_pem()
     PemFileName           name of the pem file (in)
     buffer_len            length of the converted DER binary

   DESCRIPTION
     Loads a X509 file (ca, certification, key or clr) into memory and converts
     it to a DER binary object. This object can be decoded and loaded into
     a schannel crypto context.
     If the function failed, error can be retrieved by GetLastError()
     The returned binary object must be freed by caller.

   RETURN VALUE
     NULL                  if the conversion failed or file was not found
     LPBYTE *              a pointer to a binary der object
                           buffer_len will contain the length of binary der object
*/
static LPBYTE ma_schannel_load_pem(MARIADB_PVIO *pvio, const char *PemFileName, DWORD *buffer_len)
{
  HANDLE hfile;
  char   *buffer= NULL;
  DWORD dwBytesRead= 0;
  LPBYTE der_buffer= NULL;
  DWORD der_buffer_length;

  if (buffer_len == NULL)
    return NULL;

  
  if ((hfile= CreateFile(PemFileName, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, 
                          FILE_ATTRIBUTE_NORMAL, NULL )) == INVALID_HANDLE_VALUE)
  {
    ma_schannel_set_win_error(pvio);
    return NULL;
  }

  if (!(*buffer_len = GetFileSize(hfile, NULL)))
  {
     pvio->set_error(pvio->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Invalid pem format");
     goto end;
  }

  if (!(buffer= LocalAlloc(0, *buffer_len + 1)))
  {
    pvio->set_error(pvio->mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, NULL);
    goto end;
  }

  if (!ReadFile(hfile, buffer, *buffer_len, &dwBytesRead, NULL))
  {
    ma_schannel_set_win_error(pvio);
    goto end;
  }

  CloseHandle(hfile);

  /* calculate the length of DER binary */
  if (!CryptStringToBinaryA(buffer, *buffer_len, CRYPT_STRING_BASE64HEADER,
                            NULL, &der_buffer_length, NULL, NULL))
  {
    ma_schannel_set_win_error(pvio);
    goto end;
  }
  /* allocate DER binary buffer */
  if (!(der_buffer= (LPBYTE)LocalAlloc(0, der_buffer_length)))
  {
    pvio->set_error(pvio->mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, NULL);
    goto end;
  }
  /* convert to DER binary */
  if (!CryptStringToBinaryA(buffer, *buffer_len, CRYPT_STRING_BASE64HEADER,
                            der_buffer, &der_buffer_length, NULL, NULL))
  {
    ma_schannel_set_win_error(pvio);
    goto end;
  }

  *buffer_len= der_buffer_length;
  LocalFree(buffer);
  
  return der_buffer;

end:
  if (hfile != INVALID_HANDLE_VALUE)
    CloseHandle(hfile);
  if (buffer)
    LocalFree(buffer);
  if (der_buffer)
    LocalFree(der_buffer);
  *buffer_len= 0;
  return NULL;
}
/* }}} */

/* {{{ CERT_CONTEXT *ma_schannel_create_cert_context(MARIADB_PVIO *pvio, const char *pem_file) */
/*
  Create a certification context from ca or cert file

  SYNOPSIS
    ma_schannel_create_cert_context()
    pvio                    pvio object
    pem_file               name of certificate or ca file

  DESCRIPTION
    Loads a PEM file (certificate authority or certificate) creates a certification
    context and loads the binary representation into context.
    The returned context must be freed by caller.
    If the function failed, error can be retrieved by GetLastError().

  RETURNS
    NULL                   If loading of the file or creating context failed
    CERT_CONTEXT *         A pointer to a certification context structure
*/
CERT_CONTEXT *ma_schannel_create_cert_context(MARIADB_PVIO *pvio, const char *pem_file)
{
  DWORD der_buffer_length;
  LPBYTE der_buffer= NULL;

  CERT_CONTEXT *ctx= NULL;

  /* create DER binary object from ca/certification file */
  if (!(der_buffer= ma_schannel_load_pem(pvio, pem_file, (DWORD *)&der_buffer_length)))
    goto end;
  if (!(ctx= (CERT_CONTEXT *)CertCreateCertificateContext(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
                                    der_buffer, der_buffer_length)))
    ma_schannel_set_win_error(pvio);

end:
  if (der_buffer)
    LocalFree(der_buffer);
  return ctx;
}
/* }}} */

/* {{{ PCCRL_CONTEXT ma_schannel_create_crl_context(MARIADB_PVIO *pvio, const char *pem_file) */
/*
  Create a crl context from crlfile

  SYNOPSIS
    ma_schannel_create_crl_context()
    pem_file               name of certificate or ca file

  DESCRIPTION
    Loads a certification revocation list file, creates a certification
    context and loads the binary representation into crl context.
    The returned context must be freed by caller.
    If the function failed, error can be retrieved by GetLastError().

  RETURNS
    NULL                   If loading of the file or creating context failed
    PCCRL_CONTEXT          A pointer to a certification context structure
*/
PCCRL_CONTEXT ma_schannel_create_crl_context(MARIADB_PVIO *pvio, const char *pem_file)
{
  DWORD der_buffer_length;
  LPBYTE der_buffer= NULL;

  PCCRL_CONTEXT ctx= NULL;

  /* load ca pem file into memory */
  if (!(der_buffer= ma_schannel_load_pem(pvio, pem_file, (DWORD *)&der_buffer_length)))
    goto end;
  if (!(ctx= CertCreateCRLContext(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
                                    der_buffer, der_buffer_length)))
    ma_schannel_set_win_error(pvio);
end:
  if (der_buffer)
    LocalFree(der_buffer);
  return ctx;
}
/* }}} */

/* {{{ my_bool ma_schannel_load_private_key(MARIADB_PVIO *pvio, CERT_CONTEXT *ctx, char *key_file) */
/*
  Load privte key into context

  SYNOPSIS
    ma_schannel_load_private_key()
    ctx                    pointer to a certification context
    pem_file               name of certificate or ca file

  DESCRIPTION
    Loads a certification revocation list file, creates a certification
    context and loads the binary representation into crl context.
    The returned context must be freed by caller.
    If the function failed, error can be retrieved by GetLastError().

  RETURNS
    NULL                   If loading of the file or creating context failed
    PCCRL_CONTEXT          A pointer to a certification context structure
*/

my_bool ma_schannel_load_private_key(MARIADB_PVIO *pvio, CERT_CONTEXT *ctx, char *key_file)
{
   DWORD der_buffer_len= 0;
   LPBYTE der_buffer= NULL;
   DWORD priv_key_len= 0;
   LPBYTE priv_key= NULL;
   HCRYPTPROV  crypt_prov= 0;
   HCRYPTKEY  crypt_key= 0;
   CERT_KEY_CONTEXT kpi={ 0 };
   my_bool rc= 0;

   /* load private key into der binary object */
   if (!(der_buffer= ma_schannel_load_pem(pvio, key_file, &der_buffer_len)))
     return 0;

   /* determine required buffer size for decoded private key */
   if (!CryptDecodeObjectEx(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
                            PKCS_RSA_PRIVATE_KEY,
                            der_buffer, der_buffer_len,
                            0, NULL,
                            NULL, &priv_key_len))
   {
     ma_schannel_set_win_error(pvio);
     goto end;
   }

   /* allocate buffer for decoded private key */
   if (!(priv_key= LocalAlloc(0, priv_key_len)))
   {
     pvio->set_error(pvio->mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, NULL);
     goto end;
   }

   /* decode */
   if (!CryptDecodeObjectEx(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
                            PKCS_RSA_PRIVATE_KEY,
                            der_buffer, der_buffer_len,
                            0, NULL,
                            priv_key, &priv_key_len))
   {
     ma_schannel_set_win_error(pvio);
     goto end;
   }

   /* Acquire context */
   if (!CryptAcquireContext(&crypt_prov, NULL, MS_ENHANCED_PROV, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT))
   {
     ma_schannel_set_win_error(pvio);
     goto end;
   }
   /* ... and import the private key */
   if (!CryptImportKey(crypt_prov, priv_key, priv_key_len, 0, 0, (HCRYPTKEY *)&crypt_key))
   {
     ma_schannel_set_win_error(pvio);
     goto end;
   }

   kpi.hCryptProv= crypt_prov;
   kpi.dwKeySpec = AT_KEYEXCHANGE;
   kpi.cbSize= sizeof(kpi);

   /* assign private key to certificate context */
   if (CertSetCertificateContextProperty(ctx, CERT_KEY_CONTEXT_PROP_ID, 0, &kpi))
     rc= 1;
   else
     ma_schannel_set_win_error(pvio);

end:
  if (der_buffer)
    LocalFree(der_buffer);
  if (priv_key)
  {
    if (crypt_key)
      CryptDestroyKey(crypt_key);
    LocalFree(priv_key);
  if (!rc)
    if (crypt_prov)
      CryptReleaseContext(crypt_prov, 0);
  }
  return rc;
}
/* }}} */

/* {{{ SECURITY_STATUS ma_schannel_handshake_loop(MARIADB_PVIO *pvio, my_bool InitialRead, SecBuffer *pExtraData) */
/*
  perform handshake loop

  SYNOPSIS
    ma_schannel_handshake_loop()
    pvio            Pointer to an Communication/IO structure
    InitialRead    TRUE if it's the very first read
    ExtraData      Pointer to an SecBuffer which contains extra data (sent by application)

    
*/

SECURITY_STATUS ma_schannel_handshake_loop(MARIADB_PVIO *pvio, my_bool InitialRead, SecBuffer *pExtraData)
{
  SecBufferDesc   OutBuffer, InBuffer;
  SecBuffer       InBuffers[2], OutBuffers;
  DWORD           dwSSPIFlags, dwSSPIOutFlags, cbData, cbIoBuffer;
  TimeStamp       tsExpiry;
  SECURITY_STATUS rc;
  PUCHAR          IoBuffer;
  BOOL            fDoRead;
  MARIADB_TLS     *ctls= pvio->ctls;
  SC_CTX          *sctx= (SC_CTX *)ctls->ssl;


  dwSSPIFlags = ISC_REQ_SEQUENCE_DETECT |
                ISC_REQ_REPLAY_DETECT |
                ISC_REQ_CONFIDENTIALITY |
                ISC_RET_EXTENDED_ERROR |
                ISC_REQ_ALLOCATE_MEMORY | 
                ISC_REQ_STREAM;


  /* Allocate data buffer */
  if (!(IoBuffer = LocalAlloc(LMEM_FIXED, SC_IO_BUFFER_SIZE)))
    return SEC_E_INSUFFICIENT_MEMORY;

  cbIoBuffer = 0;
  fDoRead = InitialRead;

  /* handshake loop: We will leave if handshake is finished
     or an error occurs */

  rc = SEC_I_CONTINUE_NEEDED;

  while (rc == SEC_I_CONTINUE_NEEDED ||
         rc == SEC_E_INCOMPLETE_MESSAGE ||
         rc == SEC_I_INCOMPLETE_CREDENTIALS )
  {
    /* Read data */
    if (rc == SEC_E_INCOMPLETE_MESSAGE ||
        !cbIoBuffer)
    {
      if(fDoRead)
      {
        ssize_t nbytes = pvio->methods->read(pvio, IoBuffer + cbIoBuffer, (size_t)(SC_IO_BUFFER_SIZE - cbIoBuffer));
        if (nbytes <= 0)
        {
          rc = SEC_E_INTERNAL_ERROR;
          break;
        }
        cbData = (DWORD)nbytes;
        cbIoBuffer += cbData;
      }
      else
        fDoRead = TRUE;
    }

    /* input buffers
       First buffer stores data received from server. leftover data
       will be stored in second buffer with BufferType SECBUFFER_EXTRA */

    InBuffers[0].pvBuffer   = IoBuffer;
    InBuffers[0].cbBuffer   = cbIoBuffer;
    InBuffers[0].BufferType = SECBUFFER_TOKEN;

    InBuffers[1].pvBuffer   = NULL;
    InBuffers[1].cbBuffer   = 0;
    InBuffers[1].BufferType = SECBUFFER_EMPTY;

    InBuffer.cBuffers       = 2;
    InBuffer.pBuffers       = InBuffers;
    InBuffer.ulVersion      = SECBUFFER_VERSION;


    /* output buffer */
    OutBuffers.pvBuffer  = NULL;
    OutBuffers.BufferType= SECBUFFER_TOKEN;
    OutBuffers.cbBuffer  = 0;

    OutBuffer.cBuffers      = 1;
    OutBuffer.pBuffers      = &OutBuffers;
    OutBuffer.ulVersion     = SECBUFFER_VERSION;


    rc = InitializeSecurityContextA(&sctx->CredHdl,
                                    &sctx->ctxt,
                                    NULL,
                                    dwSSPIFlags,
                                    0,
                                    SECURITY_NATIVE_DREP,
                                    &InBuffer,
                                    0,
                                    NULL,
                                    &OutBuffer,
                                    &dwSSPIOutFlags,
                                    &tsExpiry );


    if (rc == SEC_E_OK  ||
        rc == SEC_I_CONTINUE_NEEDED ||
        FAILED(rc) && (dwSSPIOutFlags & ISC_RET_EXTENDED_ERROR))
    {
      if(OutBuffers.cbBuffer && OutBuffers.pvBuffer)
      {
        ssize_t nbytes = pvio->methods->write(pvio, (uchar *)OutBuffers.pvBuffer, (size_t)OutBuffers.cbBuffer);
        if(nbytes <= 0)
        {
          FreeContextBuffer(OutBuffers.pvBuffer);
          DeleteSecurityContext(&sctx->ctxt);
          return SEC_E_INTERNAL_ERROR;
        }
        cbData= (DWORD)nbytes;
        /* Free output context buffer */
        FreeContextBuffer(OutBuffers.pvBuffer);
        OutBuffers.pvBuffer = NULL;
      }
    }
    /* check if we need to read more data */
    switch (rc) {
    case SEC_E_INCOMPLETE_MESSAGE:
      /* we didn't receive all data, so just continue loop */
      continue;
      break;
    case SEC_E_OK:
      /* handshake completed, but we need to check if extra
         data was sent (which contains encrypted application data) */
      if (InBuffers[1].BufferType == SECBUFFER_EXTRA)
      {
        if (!(pExtraData->pvBuffer= LocalAlloc(0, InBuffers[1].cbBuffer)))
          return SEC_E_INSUFFICIENT_MEMORY;
        
        MoveMemory(pExtraData->pvBuffer, IoBuffer + (cbIoBuffer - InBuffers[1].cbBuffer), InBuffers[1].cbBuffer );
        pExtraData->BufferType = SECBUFFER_TOKEN;
        pExtraData->cbBuffer   = InBuffers[1].cbBuffer;
      }
      else
      {
        pExtraData->BufferType= SECBUFFER_EMPTY;
        pExtraData->pvBuffer= NULL;
        pExtraData->cbBuffer= 0;
      }
    break;
 
    case SEC_I_INCOMPLETE_CREDENTIALS:
      /* Provided credentials didn't contain a valid client certificate.
         We will try to connect anonymously, using current credentials */
      fDoRead= FALSE;
      rc= SEC_I_CONTINUE_NEEDED;
      continue;
      break;
    default:
      if (FAILED(rc))
      {
        goto loopend;
      }
      break;
    }

    if ( InBuffers[1].BufferType == SECBUFFER_EXTRA )
    {
      MoveMemory( IoBuffer, IoBuffer + (cbIoBuffer - InBuffers[1].cbBuffer), InBuffers[1].cbBuffer );
      cbIoBuffer = InBuffers[1].cbBuffer;
    }

    cbIoBuffer = 0;
  }
loopend:
  if (FAILED(rc))
  {
    ma_schannel_set_sec_error(pvio, rc);
    DeleteSecurityContext(&sctx->ctxt);
  }
  LocalFree(IoBuffer);

  return rc;
}
/* }}} */

/* {{{ SECURITY_STATUS ma_schannel_client_handshake(MARIADB_TLS *ctls) */
/*
   performs client side handshake 

   SYNOPSIS
     ma_schannel_client_handshake()
     ctls             Pointer to a MARIADB_TLS structure

   DESCRIPTION
     initiates a client/server handshake. This function can be used
     by clients only

   RETURN
     SEC_E_OK         on success
*/

SECURITY_STATUS ma_schannel_client_handshake(MARIADB_TLS *ctls)
{
  MARIADB_PVIO *pvio;
  SECURITY_STATUS sRet;
  DWORD OutFlags;
  DWORD r;
  SC_CTX *sctx;
  SecBuffer ExtraData;
  DWORD SFlags= ISC_REQ_SEQUENCE_DETECT | ISC_REQ_REPLAY_DETECT |
                ISC_REQ_CONFIDENTIALITY | ISC_RET_EXTENDED_ERROR | 
                ISC_REQ_USE_SUPPLIED_CREDS |
                ISC_REQ_ALLOCATE_MEMORY | ISC_REQ_STREAM;
  
  SecBufferDesc	BufferOut;
  SecBuffer  BuffersOut;

  if (!ctls || !ctls->pvio)
    return 1;

  pvio= ctls->pvio;
  sctx= (SC_CTX *)ctls->ssl;

  /* Initialie securifty context */
  BuffersOut.BufferType= SECBUFFER_TOKEN;
  BuffersOut.cbBuffer= 0;
  BuffersOut.pvBuffer= NULL;


  BufferOut.cBuffers= 1;
  BufferOut.pBuffers= &BuffersOut;
  BufferOut.ulVersion= SECBUFFER_VERSION;

  sRet = InitializeSecurityContext(&sctx->CredHdl,
                                    NULL,
                                    pvio->mysql->host,
                                    SFlags,
                                    0,
                                    SECURITY_NATIVE_DREP,
                                    NULL,
                                    0,
                                    &sctx->ctxt,
                                    &BufferOut,
                                    &OutFlags,
                                    NULL);

  if(sRet != SEC_I_CONTINUE_NEEDED)
  {
    ma_schannel_set_sec_error(pvio, sRet);
    return sRet;
  }

  /* send client hello packaet */
  if(BuffersOut.cbBuffer != 0 && BuffersOut.pvBuffer != NULL)
  {  
    ssize_t nbytes = (DWORD)pvio->methods->write(pvio, (uchar *)BuffersOut.pvBuffer, (size_t)BuffersOut.cbBuffer);

    if (nbytes <= 0)
    {
      sRet= SEC_E_INTERNAL_ERROR;
      goto end;
    }
    r = (DWORD)nbytes;
  }
  sRet= ma_schannel_handshake_loop(pvio, TRUE, &ExtraData);

  /* allocate IO-Buffer for write operations: After handshake
  was successfull, we are able now to calculate payload */
  if ((sRet = QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_STREAM_SIZES, &sctx->Sizes )))
    goto end;

  sctx->IoBufferSize= SCHANNEL_PAYLOAD(sctx->Sizes);
  if (!(sctx->IoBuffer= (PUCHAR)LocalAlloc(0, sctx->IoBufferSize)))
  {
    sRet= SEC_E_INSUFFICIENT_MEMORY;
    goto end;
  }
    
  return sRet;
end:
  LocalFree(sctx->IoBuffer);
  sctx->IoBufferSize= 0;
  FreeContextBuffer(BuffersOut.pvBuffer);
  DeleteSecurityContext(&sctx->ctxt);
  return sRet;
}
/* }}} */

/* {{{ SECURITY_STATUS ma_schannel_read_decrypt(MARIADB_PVIO *pvio, PCredHandle phCreds, CtxtHandle * phContext,
                                                DWORD DecryptLength, uchar *ReadBuffer, DWORD ReadBufferSize) */
/*
  Reads encrypted data from a SSL stream and decrypts it.

  SYNOPSIS
    ma_schannel_read
    pvio              pointer to Communication IO structure
    phContext        a context handle
    DecryptLength    size of decrypted buffer
    ReadBuffer       Buffer for decrypted data
    ReadBufferSize   size of ReadBuffer


  DESCRIPTION
    Reads decrypted data from a SSL stream and encrypts it.

  RETURN
    SEC_E_OK         on success
    SEC_E_*          if an error occured
*/  

SECURITY_STATUS ma_schannel_read_decrypt(MARIADB_PVIO *pvio,
                                         PCredHandle phCreds,
                                         CtxtHandle * phContext,
                                         DWORD *DecryptLength,
                                         uchar *ReadBuffer,
                                         DWORD ReadBufferSize)
{
  ssize_t nbytes= 0;
  DWORD dwOffset= 0;
  SC_CTX *sctx;
  SECURITY_STATUS sRet= SEC_E_INCOMPLETE_MESSAGE;
  SecBufferDesc Msg;
  SecBuffer Buffers[4],
            *pData, *pExtra;
  int i;

  if (!pvio || !pvio->methods || !pvio->methods->read || !pvio->ctls || !DecryptLength)
    return SEC_E_INTERNAL_ERROR;

  sctx= (SC_CTX *)pvio->ctls->ssl;
  *DecryptLength= 0;

  if (sctx->dataBuf.cbBuffer)
  {
    /* Have unread decrypted data from the last time, copy. */
    nbytes = MIN(ReadBufferSize, sctx->dataBuf.cbBuffer);
    memcpy(ReadBuffer, sctx->dataBuf.pvBuffer, nbytes);
    sctx->dataBuf.pvBuffer = (char *)(sctx->dataBuf.pvBuffer) + nbytes;
    sctx->dataBuf.cbBuffer -= (DWORD)nbytes;
    *DecryptLength = (DWORD)nbytes;
    return SEC_E_OK;
  }


  while (1)
  {
    /* Check for any encrypted data returned by last DecryptMessage() in SECBUFFER_EXTRA buffer. */
    if (sctx->extraBuf.cbBuffer)
    {
      memmove(sctx->IoBuffer, sctx->extraBuf.pvBuffer, sctx->extraBuf.cbBuffer);
      dwOffset = sctx->extraBuf.cbBuffer;
      sctx->extraBuf.cbBuffer = 0;
    }

    nbytes= pvio->methods->read(pvio, sctx->IoBuffer + dwOffset, (size_t)(sctx->IoBufferSize - dwOffset));
    if (nbytes <= 0)
    {
      /* server closed connection, or an error */
      // todo: error 
      return SEC_E_INVALID_HANDLE;
    }
    dwOffset+= (DWORD)nbytes;

    ZeroMemory(Buffers, sizeof(SecBuffer) * 4);
    Buffers[0].pvBuffer= sctx->IoBuffer;
    Buffers[0].cbBuffer= dwOffset;

    Buffers[0].BufferType= SECBUFFER_DATA; 
    Buffers[1].BufferType=
    Buffers[2].BufferType=
    Buffers[3].BufferType= SECBUFFER_EMPTY;

    Msg.ulVersion= SECBUFFER_VERSION;    // Version number
    Msg.cBuffers= 4;
    Msg.pBuffers= Buffers;

    sRet = DecryptMessage(phContext, &Msg, 0, NULL);

    if (sRet == SEC_E_INCOMPLETE_MESSAGE)
      continue; /* Continue reading until full message arrives */

    if (sRet != SEC_E_OK)
    {
      ma_schannel_set_sec_error(pvio, sRet);
      return sRet;
    }

    pData= pExtra= NULL;
    for (i=0; i < 4; i++)
    {
      if (!pData && Buffers[i].BufferType == SECBUFFER_DATA)
        pData= &Buffers[i];
      if (!pExtra && Buffers[i].BufferType == SECBUFFER_EXTRA)
        pExtra= &Buffers[i];
      if (pData && pExtra)
        break;
    }

    if (pExtra)
    {
      /* Save preread encrypted data, will be processed next time.*/
      sctx->extraBuf.cbBuffer = pExtra->cbBuffer;
      sctx->extraBuf.pvBuffer = pExtra->pvBuffer;
    }

    if (pData && pData->cbBuffer)
    {
      /*
        Copy at most ReadBufferSize bytes to output.
        Store the rest (if any) to be processed next time.
      */
      nbytes=MIN(pData->cbBuffer, ReadBufferSize);
      memcpy((char *)ReadBuffer, pData->pvBuffer, nbytes);

 
      sctx->dataBuf.cbBuffer = pData->cbBuffer - (DWORD)nbytes;
      sctx->dataBuf.pvBuffer = (char *)pData->pvBuffer + nbytes;

      *DecryptLength = (DWORD)nbytes;
      return SEC_E_OK;
    }
    else
    {
      /*
        DecryptMessage() did not return data buffer.
        According to MSDN, this happens sometimes and is normal.
        We retry the read/decrypt in this case.
      */
      dwOffset = 0;
    }
  }
}
/* }}} */

my_bool ma_schannel_verify_certs(SC_CTX *sctx)
{
  SECURITY_STATUS sRet;
  MYSQL *mysql=sctx->mysql;

  MARIADB_PVIO *pvio= mysql->net.pvio;
  const char *ca_file= mysql->options.ssl_ca;
  const char *crl_file= mysql->options.extension ? mysql->options.extension->ssl_crl : NULL;
  PCCERT_CONTEXT pServerCert= NULL;
  CRL_CONTEXT *crl_ctx= NULL;
  CERT_CONTEXT *ca_ctx= NULL;
  int ret= 0;

  if (!ca_file && !crl_file)
    return 1;

  if (ca_file && !(ca_ctx = ma_schannel_create_cert_context(pvio, ca_file)))
    goto end;

  if (crl_file && !(crl_ctx= (CRL_CONTEXT *)ma_schannel_create_crl_context(pvio, mysql->options.extension->ssl_crl)))
    goto end;
  
  if ((sRet= QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_REMOTE_CERT_CONTEXT, (PVOID)&pServerCert)) != SEC_E_OK)
  {
    ma_schannel_set_sec_error(pvio, sRet);
    goto end;
  }

  if (ca_ctx)
  {
    DWORD flags = CERT_STORE_SIGNATURE_FLAG | CERT_STORE_TIME_VALIDITY_FLAG;
    if (!CertVerifySubjectCertificateContext(pServerCert, ca_ctx, &flags))
    {
      ma_schannel_set_win_error(pvio);
      goto end;
    }

    if (flags)
    {
      if ((flags & CERT_STORE_SIGNATURE_FLAG) != 0)
        pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Certificate signature check failed");
      else if ((flags & CERT_STORE_REVOCATION_FLAG) != 0)
        pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: certificate was revoked");
      else if ((flags & CERT_STORE_TIME_VALIDITY_FLAG) != 0)
        pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: certificate has expired");
      else
        pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Unknown error during certificate validation");
      goto end;
    }
  }


  /* Check  certificates in the certificate chain have been revoked. */
  if (crl_ctx)
  {
    if (!CertVerifyCRLRevocation(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, pServerCert->pCertInfo, 1, &crl_ctx->pCrlInfo))
    {
      pvio->set_error(pvio->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: CRL Revocation test failed");
      goto end;
    }
  }
  ret= 1;

end:
  if (crl_ctx)
  {
    CertFreeCRLContext(crl_ctx);
  }
  if (ca_ctx)
  {
    CertFreeCertificateContext(ca_ctx);
  }
  if (pServerCert)
  {
    CertFreeCertificateContext(pServerCert);
  }
  return ret;
}


/* {{{ size_t ma_schannel_write_encrypt(MARIADB_PVIO *pvio, PCredHandle phCreds, CtxtHandle * phContext) */
/*
  Decrypts data and write to SSL stream
  SYNOPSIS
    ma_schannel_write_decrypt
    pvio              pointer to Communication IO structure
    phContext        a context handle
    DecryptLength    size of decrypted buffer
    ReadBuffer       Buffer for decrypted data
    ReadBufferSize   size of ReadBuffer

  DESCRIPTION
    Write encrypted data to SSL stream.

  RETURN
    SEC_E_OK         on success
    SEC_E_*          if an error occured
*/ 
ssize_t ma_schannel_write_encrypt(MARIADB_PVIO *pvio,
                                 uchar *WriteBuffer,
                                 size_t WriteBufferSize)
{
  SECURITY_STATUS scRet;
  SecBufferDesc Message;
  SecBuffer Buffers[4];
  DWORD cbMessage;
  PBYTE pbMessage;
  SC_CTX *sctx= (SC_CTX *)pvio->ctls->ssl;
  size_t payload;
  ssize_t nbytes;
  DWORD write_size;

  payload= MIN(WriteBufferSize, sctx->Sizes.cbMaximumMessage);

  memcpy(&sctx->IoBuffer[sctx->Sizes.cbHeader], WriteBuffer, payload);
  pbMessage = sctx->IoBuffer + sctx->Sizes.cbHeader; 
  cbMessage = (DWORD)payload;
  
  Buffers[0].pvBuffer     = sctx->IoBuffer;
  Buffers[0].cbBuffer     = sctx->Sizes.cbHeader;
  Buffers[0].BufferType   = SECBUFFER_STREAM_HEADER;    // Type of the buffer

  Buffers[1].pvBuffer     = &sctx->IoBuffer[sctx->Sizes.cbHeader];
  Buffers[1].cbBuffer     = (DWORD)payload;
  Buffers[1].BufferType   = SECBUFFER_DATA;

  Buffers[2].pvBuffer     = &sctx->IoBuffer[sctx->Sizes.cbHeader] + payload;
  Buffers[2].cbBuffer     = sctx->Sizes.cbTrailer;
  Buffers[2].BufferType   = SECBUFFER_STREAM_TRAILER;

  Buffers[3].pvBuffer     = SECBUFFER_EMPTY;                    // Pointer to buffer 4
  Buffers[3].cbBuffer     = SECBUFFER_EMPTY;                    // length of buffer 4
  Buffers[3].BufferType   = SECBUFFER_EMPTY;                    // Type of the buffer 4


  Message.ulVersion       = SECBUFFER_VERSION;
  Message.cBuffers        = 4;
  Message.pBuffers        = Buffers;
  if ((scRet = EncryptMessage(&sctx->ctxt, 0, &Message, 0))!= SEC_E_OK)
    return -1;
  write_size = Buffers[0].cbBuffer + Buffers[1].cbBuffer + Buffers[2].cbBuffer;
  nbytes = pvio->methods->write(pvio, sctx->IoBuffer, write_size);
  return nbytes == write_size ? payload : -1;
}
/* }}} */

extern char *ssl_protocol_version[5];

/* {{{ ma_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version) */
my_bool ma_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version)
{
  SC_CTX *sctx;
  SecPkgContext_ConnectionInfo ConnectionInfo;
  if (!ctls->ssl)
    return 1;

  sctx= (SC_CTX *)ctls->ssl;

  if (QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_CONNECTION_INFO, &ConnectionInfo) != SEC_E_OK)
    return 1;

  switch(ConnectionInfo.dwProtocol)
  {
  case SP_PROT_SSL3_CLIENT:
    version->iversion= 1;
    break;
  case SP_PROT_TLS1_CLIENT:
    version->iversion= 2;
    break;
  case SP_PROT_TLS1_1_CLIENT:
    version->iversion= 3;
    break;
  case SP_PROT_TLS1_2_CLIENT:
    version->iversion= 4;
    break;
  default:
    version->iversion= 0;
    break;
  }
  version->cversion= ssl_protocol_version[version->iversion];
  return 0;
}
/* }}} */
