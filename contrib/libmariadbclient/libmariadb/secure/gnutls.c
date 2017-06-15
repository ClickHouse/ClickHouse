/************************************************************************************
  Copyright (C) 2014 MariaDB Corporation AB

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

 *************************************************************************************/
#ifdef HAVE_GNUTLS

#include <gnutls/gnutls.h>
#include <gnutls/x509.h>
#include <gnutls/abstract.h>
#include <ma_global.h>
#include <ma_sys.h>
#include <ma_common.h>
#include <ma_pvio.h>
#include <errmsg.h>
#include <ma_pthread.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <ma_tls.h>

pthread_mutex_t LOCK_gnutls_config;

extern my_bool ma_tls_initialized;
extern unsigned int mariadb_deinitialize_ssl;

enum ma_pem_type {
  MA_TLS_PEM_CERT= 0,
  MA_TLS_PEM_KEY,
  MA_TLS_PEM_CA,
  MA_TLS_PEM_CRL
};

static int my_verify_callback(gnutls_session_t ssl);

struct st_gnutls_data {
  MYSQL *mysql;
  gnutls_privkey_t key;
  gnutls_pcert_st cert;
};

struct st_cipher_map {
  unsigned char sid[2];
  const char *iana_name;
  const char *openssl_name;
  const char *gnutls_name;
};

const struct st_cipher_map tls_ciphers[]=
{
  { {0x00, 0x01},
    "TLS_RSA_WITH_NULL_MD5",
     NULL,
    "TLS_RSA_NULL_MD5"},
  { {0x00, 0x02},
    "TLS_RSA_WITH_NULL_SHA",
     NULL,
    "TLS_RSA_NULL_SHA1"},
  { {0x00, 0x3B},
    "TLS_RSA_WITH_NULL_SHA256",
     NULL,
    "TLS_RSA_NULL_SHA256"},
  { {0x00, 0x05},
    "TLS_RSA_WITH_RC4_128_SHA",
     NULL,
    "TLS_RSA_ARCFOUR_128_SHA1"},
  { {0x00, 0x04},
    "TLS_RSA_WITH_RC4_128_MD5",
     NULL,
    "TLS_RSA_ARCFOUR_128_MD5"},
  { {0x00, 0x0A},
    "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
    "DES-CBC3-SHA",
    "TLS_RSA_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x2F},
    "TLS_RSA_WITH_AES_128_CBC_SHA",
    "AES128-SHA",
    "TLS_RSA_AES_128_CBC_SHA1"},
  { {0x00, 0x35},
    "TLS_RSA_WITH_AES_256_CBC_SHA",
    "AES256-SHA",
    "TLS_RSA_AES_256_CBC_SHA1"},
  { {0x00, 0xBA},
    "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256",
    "CAMELLIA128-SHA256",
    "TLS_RSA_CAMELLIA_128_CBC_SHA256"},
  { {0x00, 0xC0},
    "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256",
     NULL,
    "TLS_RSA_CAMELLIA_256_CBC_SHA256"},
  { {0x00, 0x41},
    "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA",
    "CAMELLIA128-SHA",
    "TLS_RSA_CAMELLIA_128_CBC_SHA1"},
  { {0x00, 0x84},
    "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA",
    "CAMELLIA256-SHA",
    "TLS_RSA_CAMELLIA_256_CBC_SHA1"},
  { {0x00, 0x3C},
    "TLS_RSA_WITH_AES_128_CBC_SHA256",
    "AES128-SHA256",
    "TLS_RSA_AES_128_CBC_SHA256"},
  { {0x00, 0x3D},
    "TLS_RSA_WITH_AES_256_CBC_SHA256",
    "AES256-SHA256",
    "TLS_RSA_AES_256_CBC_SHA256"},
  { {0x00, 0x9C},
    "TLS_RSA_WITH_AES_128_GCM_SHA256",
    "AES128-GCM-SHA256",
    "TLS_RSA_AES_128_GCM_SHA256"},
  { {0x00, 0x9D},
    "TLS_RSA_WITH_AES_256_GCM_SHA384",
    "AES256-GCM-SHA384",
    "TLS_RSA_AES_256_GCM_SHA384"},
  { {0xC0, 0x7A},
    "TLS_RSA_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_RSA_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x7B},
    "TLS_RSA_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_RSA_CAMELLIA_256_GCM_SHA384"},
  { {0xC0, 0x9C},
    "TLS_RSA_WITH_AES_128_CCM",
     NULL,
    "TLS_RSA_AES_128_CCM"},
  { {0xC0, 0x9D},
    "TLS_RSA_WITH_AES_256_CCM",
     NULL,
    "TLS_RSA_AES_256_CCM"},
  { {0xC0, 0xA0},
    "TLS_RSA_WITH_AES_128_CCM_8",
     NULL,
    "TLS_RSA_AES_128_CCM_8"},
  { {0xC0, 0xA1},
    "TLS_RSA_WITH_AES_256_CCM_8",
     NULL,
    "TLS_RSA_AES_256_CCM_8"},
  { {0x00, 0x66},
    "TLS_DHE_DSS_WITH_RC4_128_SHA",
    NULL,
    "TLS_DHE_DSS_ARCFOUR_128_SHA1"},
  { {0x00, 0x13},
    "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA",
     NULL,
    "TLS_DHE_DSS_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x32},
    "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
     NULL,
    "TLS_DHE_DSS_AES_128_CBC_SHA1"},
  { {0x00, 0x38},
    "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
     NULL,
    "TLS_DHE_DSS_AES_256_CBC_SHA1"},
  { {0x00, 0xBD},
    "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_128_CBC_SHA256"},
  { {0x00, 0xC3},
    "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA256",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_256_CBC_SHA256"},
  { {0x00, 0x44},
    "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_128_CBC_SHA1"},
  { {0x00, 0x87},
    "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_256_CBC_SHA1"},
  { {0x00, 0x40},
    "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
     NULL,
    "TLS_DHE_DSS_AES_128_CBC_SHA256"},
  { {0x00, 0x6A},
    "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
     NULL,
    "TLS_DHE_DSS_AES_256_CBC_SHA256"},
  { {0x00, 0xA2},
    "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256",
     NULL,
    "TLS_DHE_DSS_AES_128_GCM_SHA256"},
  { {0x00, 0xA3},
    "TLS_DHE_DSS_WITH_AES_256_GCM_SHA384",
     NULL,
    "TLS_DHE_DSS_AES_256_GCM_SHA384"},
  { {0xC0, 0x80},
    "TLS_DHE_DSS_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x81},
    "TLS_DHE_DSS_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_DHE_DSS_CAMELLIA_256_GCM_SHA384"},
  { {0x00, 0x16},
    "TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
    "EDH-RSA-DES-CBC3-SHA",
    "TLS_DHE_RSA_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x33},
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
    "DHE-RSA-AES128-SHA",
    "TLS_DHE_RSA_AES_128_CBC_SHA1"},
  { {0x00, 0x39},
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
    "DHE-RSA-AES256-SHA",
    "TLS_DHE_RSA_AES_256_CBC_SHA1"},
  { {0x00, 0xBE},
    "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_DHE_RSA_CAMELLIA_128_CBC_SHA256"},
  { {0x00, 0xC4},
    "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256",
     NULL,
    "TLS_DHE_RSA_CAMELLIA_256_CBC_SHA256"},
  { {0x00, 0x45},
    "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA",
    "DHE-RSA-CAMELLIA128-SHA",
    "TLS_DHE_RSA_CAMELLIA_128_CBC_SHA1"},
  { {0x00, 0x88},
    "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA",
    "DHE-RSA-CAMELLIA256-SHA",
    "TLS_DHE_RSA_CAMELLIA_256_CBC_SHA1"},
  { {0x00, 0x67},
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
    "DHE-RSA-AES128-SHA256",
    "TLS_DHE_RSA_AES_128_CBC_SHA256"},
  { {0x00, 0x6B},
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
    "DHE-RSA-AES256-SHA256",
    "TLS_DHE_RSA_AES_256_CBC_SHA256"},
  { {0x00, 0x9E},
    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
    "DHE-RSA-AES128-GCM-SHA256",
    "TLS_DHE_RSA_AES_128_GCM_SHA256"},
  { {0x00, 0x9F},
    "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
    "DHE-RSA-AES256-GCM-SHA384",
    "TLS_DHE_RSA_AES_256_GCM_SHA384"},
  { {0xC0, 0x7C},
    "TLS_DHE_RSA_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_DHE_RSA_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x7D},
    "TLS_DHE_RSA_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_DHE_RSA_CAMELLIA_256_GCM_SHA384"},
  { {0xCC, 0xAA},
    "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
    "DHE-RSA-CHACHA20-POLY1305",
    "TLS_DHE_RSA_CHACHA20_POLY1305"},
  { {0xC0, 0x9E},
    "TLS_DHE_RSA_WITH_AES_128_CCM",
     NULL,
    "TLS_DHE_RSA_AES_128_CCM"},
  { {0xC0, 0x9F},
    "TLS_DHE_RSA_WITH_AES_256_CCM",
     NULL,
    "TLS_DHE_RSA_AES_256_CCM"},
  { {0xC0, 0xA2},
    "TLS_DHE_RSA_WITH_AES_128_CCM_8",
     NULL,
    "TLS_DHE_RSA_AES_128_CCM_8"},
  { {0xC0, 0xA3},
    "TLS_DHE_RSA_WITH_AES_256_CCM_8",
     NULL,
    "TLS_DHE_RSA_AES_256_CCM_8"},
  { {0xC0, 0x10},
    "TLS_ECDHE_RSA_WITH_",
     NULL,
    "TLS_ECDHE_RSA_NULL_SHA1"},
  { {0xC0, 0x12},
    "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
    "ECDHE-RSA-DES-CBC3-SHA",
    "TLS_ECDHE_RSA_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x13},
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
    "ECDHE-RSA-AES128-SHA",
    "TLS_ECDHE_RSA_AES_128_CBC_SHA1"},
  { {0xC0, 0x14},
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    "ECDHE-RSA-AES256-SHA",
    "TLS_ECDHE_RSA_AES_256_CBC_SHA1"},
  { {0xC0, 0x28},
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
    "ECDHE-RSA-AES256-SHA384",
    "TLS_ECDHE_RSA_AES_256_CBC_SHA384"},
  { {0xC0, 0x11},
    "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
     NULL,
    "TLS_ECDHE_RSA_ARCFOUR_128_SHA1"},
  { {0xC0, 0x76},
    "TLS_ECDHE_RSA_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_ECDHE_RSA_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x77},
    "TLS_ECDHE_RSA_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_ECDHE_RSA_CAMELLIA_256_CBC_SHA384"},
  { {0xC0, 0x06},
    "TLS_ECDHE_ECDSA_WITH_",
     NULL,
    "TLS_ECDHE_ECDSA_NULL_SHA1"},
  { {0xC0, 0x08},
    "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
    "ECDHE-ECDSA-DES-CBC3-SHA",
    "TLS_ECDHE_ECDSA_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x09},
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
    "ECDHE-ECDSA-AES128-SHA",
    "TLS_ECDHE_ECDSA_AES_128_CBC_SHA1"},
  { {0xC0, 0x0A},
    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
    "ECDHE-ECDSA-AES256-SHA",
    "TLS_ECDHE_ECDSA_AES_256_CBC_SHA1"},
  { {0xC0, 0x07},
    "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
     NULL,
    "TLS_ECDHE_ECDSA_ARCFOUR_128_SHA1"},
  { {0xC0, 0x72},
    "TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_ECDHE_ECDSA_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x73},
    "TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_ECDHE_ECDSA_CAMELLIA_256_CBC_SHA384"},
  { {0xC0, 0x23},
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
    "ECDHE-ECDSA-AES128-SHA256",
    "TLS_ECDHE_ECDSA_AES_128_CBC_SHA256"},
  { {0xC0, 0x27},
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "ECDHE-RSA-AES128-SHA256",
    "TLS_ECDHE_RSA_AES_128_CBC_SHA256"},
  { {0xC0, 0x86},
    "TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_ECDHE_ECDSA_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x87},
    "TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_ECDHE_ECDSA_CAMELLIA_256_GCM_SHA384"},
  { {0xC0, 0x2B},
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "TLS_ECDHE_ECDSA_AES_128_GCM_SHA256"},
  { {0xC0, 0x2C},
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "TLS_ECDHE_ECDSA_AES_256_GCM_SHA384"},
  { {0xC0, 0x2F},
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "TLS_ECDHE_RSA_AES_128_GCM_SHA256"},
  { {0xC0, 0x30},
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384",
    "TLS_ECDHE_RSA_AES_256_GCM_SHA384"},
  { {0xC0, 0x24},
    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
    "ECDHE-ECDSA-AES256-SHA384",
    "TLS_ECDHE_ECDSA_AES_256_CBC_SHA384"},
  { {0xC0, 0x8A},
    "TLS_ECDHE_RSA_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_ECDHE_RSA_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x8B},
    "TLS_ECDHE_RSA_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_ECDHE_RSA_CAMELLIA_256_GCM_SHA384"},
  { {0xCC, 0xA8},
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
    "ECDHE-RSA-CHACHA20-POLY1305",
    "TLS_ECDHE_RSA_CHACHA20_POLY1305"},
  { {0xCC, 0xA9},
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
    "ECDHE-ECDSA-CHACHA20-POLY1305",
    "TLS_ECDHE_ECDSA_CHACHA20_POLY1305"},
  { {0xC0, 0xAC},
    "TLS_ECDHE_ECDSA_WITH_AES_128_CCM",
     NULL,
    "TLS_ECDHE_ECDSA_AES_128_CCM"},
  { {0xC0, 0xAD},
    "TLS_ECDHE_ECDSA_WITH_AES_256_CCM",
     NULL,
    "TLS_ECDHE_ECDSA_AES_256_CCM"},
  { {0xC0, 0xAE},
    "TLS_ECDHE_ECDSA_WITH_AES_128_CCM_8",
     NULL,
    "TLS_ECDHE_ECDSA_AES_128_CCM_8"},
  { {0xC0, 0xAF},
    "TLS_ECDHE_ECDSA_WITH_AES_256_CCM_8",
     NULL,
    "TLS_ECDHE_ECDSA_AES_256_CCM_8"},
  { {0xC0, 0x34},
    "TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA",
    "ECDHE-PSK-3DES-EDE-CBC-SHA",
    "TLS_ECDHE_PSK_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x35},
    "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA",
    "ECDHE-PSK-AES128-CBC-SHA",
    "TLS_ECDHE_PSK_AES_128_CBC_SHA1"},
  { {0xC0, 0x36},
    "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA",
    "ECDHE-PSK-AES256-CBC-SHA",
    "TLS_ECDHE_PSK_AES_256_CBC_SHA1"},
  { {0xC0, 0x37},
    "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256",
    "ECDHE-PSK-AES128-CBC-SHA256",
    "TLS_ECDHE_PSK_AES_128_CBC_SHA256"},
  { {0xC0, 0x38},
    "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384",
    "ECDHE-PSK-AES256-CBC-SHA384",
    "TLS_ECDHE_PSK_AES_256_CBC_SHA384"},
  { {0xC0, 0x33},
    "TLS_ECDHE_PSK_WITH_RC4_128_SHA",
     NULL,
    "TLS_ECDHE_PSK_ARCFOUR_128_SHA1"},
  { {0xC0, 0x39},
    "TLS_ECDHE_PSK_WITH_NULL_SHA",
     NULL,
    "TLS_ECDHE_PSK_NULL_SHA1"},
  { {0xC0, 0x3A},
    "TLS_ECDHE_PSK_WITH_NULL_SHA256",
     NULL,
    "TLS_ECDHE_PSK_NULL_SHA256"},
  { {0xC0, 0x3B},
    "TLS_ECDHE_PSK_WITH_NULL_SHA384",
     NULL,
    "TLS_ECDHE_PSK_NULL_SHA384"},
  { {0xC0, 0x9A},
    "TLS_ECDHE_PSK_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_ECDHE_PSK_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x9B},
    "TLS_ECDHE_PSK_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_ECDHE_PSK_CAMELLIA_256_CBC_SHA384"},
  { {0x00, 0x8A},
    "TLS_PSK_WITH_RC4_128_SHA",
     NULL,
    "TLS_PSK_ARCFOUR_128_SHA1"},
  { {0x00, 0x8B},
    "TLS_PSK_WITH_3DES_EDE_CBC_SHA",
    "PSK-3DES-EDE-CBC-SHA",
    "TLS_PSK_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x8C},
    "TLS_PSK_WITH_AES_128_CBC_SHA",
    "PSK-AES128-CBC-SHA",
    "TLS_PSK_AES_128_CBC_SHA1"},
  { {0x00, 0x8D},
    "TLS_PSK_WITH_AES_256_CBC_SHA",
    "PSK-AES256-CBC-SHA",
    "TLS_PSK_AES_256_CBC_SHA1"},
  { {0x00, 0xAE},
    "TLS_PSK_WITH_AES_128_CBC_SHA256",
    "PSK-AES128-CBC-SHA256",
    "TLS_PSK_AES_128_CBC_SHA256"},
  { {0x00, 0xA9},
    "TLS_PSK_WITH_AES_256_GCM_SHA384",
    "PSK-AES256-GCM-SHA384",
    "TLS_PSK_AES_256_GCM_SHA384"},
  { {0xC0, 0x8E},
    "TLS_PSK_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_PSK_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x8F},
    "TLS_PSK_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_PSK_CAMELLIA_256_GCM_SHA384"},
  { {0x00, 0xA8},
    "TLS_PSK_WITH_AES_128_GCM_SHA256",
    "PSK-AES128-GCM-SHA256",
    "TLS_PSK_AES_128_GCM_SHA256"},
  { {0x00, 0x2C},
    "TLS_PSK_WITH_",
     NULL,
    "TLS_PSK_NULL_SHA1"},
  { {0x00, 0xB0},
    "TLS_PSK_WITH_",
     NULL,
    "TLS_PSK_NULL_SHA256"},
  { {0xC0, 0x94},
    "TLS_PSK_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_PSK_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x95},
    "TLS_PSK_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_PSK_CAMELLIA_256_CBC_SHA384"},
  { {0x00, 0xAF},
    "TLS_PSK_WITH_AES_256_CBC_SHA384",
    "PSK-AES256-CBC-SHA384",
    "TLS_PSK_AES_256_CBC_SHA384"},
  { {0x00, 0xB1},
    "TLS_PSK_WITH_",
     NULL,
    "TLS_PSK_NULL_SHA384"},
  { {0x00, 0x92},
    "TLS_RSA_PSK_WITH_RC4_128_SHA",
     NULL,
    "TLS_RSA_PSK_ARCFOUR_128_SHA1"},
  { {0x00, 0x93},
    "TLS_RSA_PSK_WITH_3DES_EDE_CBC_SHA",
    "RSA-PSK-3DES-EDE-CBC-SHA",
    "TLS_RSA_PSK_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x94},
    "TLS_RSA_PSK_WITH_AES_128_CBC_SHA",
    "RSA-PSK-AES128-CBC-SHA",
    "TLS_RSA_PSK_AES_128_CBC_SHA1"},
  { {0x00, 0x95},
    "TLS_RSA_PSK_WITH_AES_256_CBC_SHA",
    "RSA-PSK-AES256-CBC-SHA",
    "TLS_RSA_PSK_AES_256_CBC_SHA1"},
  { {0xC0, 0x92},
    "TLS_RSA_PSK_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_RSA_PSK_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x93},
    "TLS_RSA_PSK_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_RSA_PSK_CAMELLIA_256_GCM_SHA384"},
  { {0x00, 0xAC},
    "TLS_RSA_PSK_WITH_AES_128_GCM_SHA256",
    "RSA-PSK-AES128-GCM-SHA256",
    "TLS_RSA_PSK_AES_128_GCM_SHA256"},
  { {0x00, 0xB6},
    "TLS_RSA_PSK_WITH_AES_128_CBC_SHA256",
    "RSA-PSK-AES128-CBC-SHA256",
    "TLS_RSA_PSK_AES_128_CBC_SHA256"},
  { {0x00, 0x2E},
    "TLS_RSA_PSK_WITH_NULL_SHA",
     NULL,
    "TLS_RSA_PSK_NULL_SHA1"},
  { {0x00, 0xB8},
    "TLS_RSA_PSK_WITH_",
     NULL,
    "TLS_RSA_PSK_NULL_SHA256"},
  { {0x00, 0xAD},
    "TLS_RSA_PSK_WITH_AES_256_GCM_SHA384",
    "RSA-PSK-AES256-GCM-SHA384",
    "TLS_RSA_PSK_AES_256_GCM_SHA384"},
  { {0x00, 0xB7},
    "TLS_RSA_PSK_WITH_AES_256_CBC_SHA384",
    "RSA-PSK-AES256-CBC-SHA384",
    "TLS_RSA_PSK_AES_256_CBC_SHA384"},
  { {0x00, 0xB9},
    "TLS_RSA_PSK_WITH_",
     NULL,
    "TLS_RSA_PSK_NULL_SHA384"},
  { {0xC0, 0x98},
    "TLS_RSA_PSK_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_RSA_PSK_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x99},
    "TLS_RSA_PSK_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_RSA_PSK_CAMELLIA_256_CBC_SHA384"},
  { {0x00, 0x8E},
    "TLS_DHE_PSK_WITH_RC4_128_SHA",
     NULL,
    "TLS_DHE_PSK_ARCFOUR_128_SHA1"},
  { {0x00, 0x8F},
    "TLS_DHE_PSK_WITH_3DES_EDE_CBC_SHA",
    "DHE-PSK-3DES-EDE-CBC-SHA",
    "TLS_DHE_PSK_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x90},
    "TLS_DHE_PSK_WITH_AES_128_CBC_SHA",
    "DHE-PSK-AES128-CBC-SHA",
    "TLS_DHE_PSK_AES_128_CBC_SHA1"},
  { {0x00, 0x91},
    "TLS_DHE_PSK_WITH_AES_256_CBC_SHA",
    "DHE-PSK-AES256-CBC-SHA",
    "TLS_DHE_PSK_AES_256_CBC_SHA1"},
  { {0x00, 0xB2},
    "TLS_DHE_PSK_WITH_AES_128_CBC_SHA256",
    "DHE-PSK-AES128-CBC-SHA256",
    "TLS_DHE_PSK_AES_128_CBC_SHA256"},
  { {0x00, 0xAA},
    "TLS_DHE_PSK_WITH_AES_128_GCM_SHA256",
    "DHE-PSK-AES128-GCM-SHA256",
    "TLS_DHE_PSK_AES_128_GCM_SHA256"},
  { {0x00, 0x2D},
    "TLS_DHE_PSK_WITH_",
     NULL,
    "TLS_DHE_PSK_NULL_SHA1"},
  { {0x00, 0xB4},
    "TLS_DHE_PSK_WITH_",
     NULL,
    "TLS_DHE_PSK_NULL_SHA256"},
  { {0x00, 0xB5},
    "TLS_DHE_PSK_WITH_",
     NULL,
    "TLS_DHE_PSK_NULL_SHA384"},
  { {0x00, 0xB3},
    "TLS_DHE_PSK_WITH_AES_256_CBC_SHA384",
    "DHE-PSK-AES256-CBC-SHA384",
    "TLS_DHE_PSK_AES_256_CBC_SHA384"},
  { {0x00, 0xAB},
    "TLS_DHE_PSK_WITH_AES_256_GCM_SHA384",
    "DHE-PSK-AES256-GCM-SHA384",
    "TLS_DHE_PSK_AES_256_GCM_SHA384"},
  { {0xC0, 0x96},
    "TLS_DHE_PSK_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_DHE_PSK_CAMELLIA_128_CBC_SHA256"},
  { {0xC0, 0x97},
    "TLS_DHE_PSK_WITH_CAMELLIA_256_CBC_SHA384",
     NULL,
    "TLS_DHE_PSK_CAMELLIA_256_CBC_SHA384"},
  { {0xC0, 0x90},
    "TLS_DHE_PSK_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_DHE_PSK_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x91},
    "TLS_DHE_PSK_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_DHE_PSK_CAMELLIA_256_GCM_SHA384"},
  { {0xC0, 0xA4},
    "TLS_PSK_WITH_AES_128_CCM",
     NULL,
    "TLS_PSK_AES_128_CCM"},
  { {0xC0, 0xA5},
    "TLS_PSK_WITH_AES_256_CCM",
     NULL,
    "TLS_PSK_AES_256_CCM"},
  { {0xC0, 0xA6},
    "TLS_DHE_PSK_WITH_AES_128_CCM",
     NULL,
    "TLS_DHE_PSK_AES_128_CCM"},
  { {0xC0, 0xA7},
    "TLS_DHE_PSK_WITH_AES_256_CCM",
     NULL,
    "TLS_DHE_PSK_AES_256_CCM"},
  { {0xC0, 0xA8},
    "TLS_PSK_WITH_AES_128_CCM_8",
     NULL,
    "TLS_PSK_AES_128_CCM_8"},
  { {0xC0, 0xA9},
    "TLS_PSK_WITH_AES_256_CCM_8",
     NULL,
    "TLS_PSK_AES_256_CCM_8"},
  { {0xC0, 0xAA},
    "TLS_PSK_DHE_WITH_AES_128_CCM_8",
     NULL,
    "TLS_DHE_PSK_AES_128_CCM_8"},
  { {0xC0, 0xAB},
    "TLS_PSK_DHE_WITH_AES_256_CCM_8",
     NULL,
    "TLS_DHE_PSK_AES_256_CCM_8"},
  { {0xCC, 0xAD},
    "TLS_DHE_PSK_WITH_CHACHA20_POLY1305_SHA256",
    "DHE-PSK-CHACHA20-POLY1305",
    "TLS_DHE_PSK_CHACHA20_POLY1305"},
  { {0xCC, 0xAC},
    "TLS_ECDHE_PSK_WITH_CHACHA20_POLY1305_SHA256",
    "ECDHE-PSK-CHACHA20-POLY1305",
    "TLS_ECDHE_PSK_CHACHA20_POLY1305"},
  { {0xCC, 0xAE},
    "TLS_RSA_PSK_WITH_CHACHA20_POLY1305_SHA256",
    "RSA-PSK-CHACHA20-POLY1305",
    "TLS_RSA_PSK_CHACHA20_POLY1305"},
  { {0xCC, 0xAB},
    "TLS_PSK_WITH_CHACHA20_POLY1305_SHA256",
    "PSK-CHACHA20-POLY1305",
    "TLS_PSK_CHACHA20_POLY1305"},
  { {0x00, 0x18},
    "TLS_DH_anon_WITH_RC4_128_MD5",
     NULL,
    "TLS_DH_ANON_ARCFOUR_128_MD5"},
  { {0x00, 0x1B},
    "TLS_DH_anon_WITH_3DES_EDE_CBC_SHA",
     NULL,
    "TLS_DH_ANON_3DES_EDE_CBC_SHA1"},
  { {0x00, 0x34},
    "TLS_DH_anon_WITH_AES_128_CBC_SHA",
     NULL,
    "TLS_DH_ANON_AES_128_CBC_SHA1"},
  { {0x00, 0x3A},
    "TLS_DH_anon_WITH_AES_256_CBC_SHA",
     NULL,
    "TLS_DH_ANON_AES_256_CBC_SHA1"},
  { {0x00, 0xBF},
    "TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA256",
     NULL,
    "TLS_DH_ANON_CAMELLIA_128_CBC_SHA256"},
  { {0x00, 0xC5},
    "TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA256",
     NULL,
    "TLS_DH_ANON_CAMELLIA_256_CBC_SHA256"},
  { {0x00, 0x46},
    "TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA",
     NULL,
    "TLS_DH_ANON_CAMELLIA_128_CBC_SHA1"},
  { {0x00, 0x89},
    "TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA",
     NULL,
    "TLS_DH_ANON_CAMELLIA_256_CBC_SHA1"},
  { {0x00, 0x6C},
    "TLS_DH_anon_WITH_AES_128_CBC_SHA256",
     NULL,
    "TLS_DH_ANON_AES_128_CBC_SHA256"},
  { {0x00, 0x6D},
    "TLS_DH_anon_WITH_AES_256_CBC_SHA256",
     NULL,
    "TLS_DH_ANON_AES_256_CBC_SHA256"},
  { {0x00, 0xA6},
    "TLS_DH_anon_WITH_AES_128_GCM_SHA256",
     NULL,
    "TLS_DH_ANON_AES_128_GCM_SHA256"},
  { {0x00, 0xA7},
    "TLS_DH_anon_WITH_AES_256_GCM_SHA384",
     NULL,
    "TLS_DH_ANON_AES_256_GCM_SHA384"},
  { {0xC0, 0x84},
    "TLS_DH_anon_WITH_CAMELLIA_128_GCM_SHA256",
     NULL,
    "TLS_DH_ANON_CAMELLIA_128_GCM_SHA256"},
  { {0xC0, 0x85},
    "TLS_DH_anon_WITH_CAMELLIA_256_GCM_SHA384",
     NULL,
    "TLS_DH_ANON_CAMELLIA_256_GCM_SHA384"},
  { {0xC0, 0x15},
    "TLS_ECDH_anon_WITH_",
     NULL,
    "TLS_ECDH_ANON_NULL_SHA1"},
  { {0xC0, 0x17},
    "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA",
     NULL,
    "TLS_ECDH_ANON_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x18},
    "TLS_ECDH_anon_WITH_AES_128_CBC_SHA",
     NULL,
    "TLS_ECDH_ANON_AES_128_CBC_SHA1"},
  { {0xC0, 0x19},
    "TLS_ECDH_anon_WITH_AES_256_CBC_SHA",
     NULL,
    "TLS_ECDH_ANON_AES_256_CBC_SHA1"},
  { {0xC0, 0x16},
    "TLS_ECDH_anon_WITH_RC4_128_SHA",
     NULL,
    "TLS_ECDH_ANON_ARCFOUR_128_SHA1"},
  { {0xC0, 0x1A},
    "TLS_SRP_SHA_WITH_3DES_EDE_CBC_SHA",
    "SRP-3DES-EDE-CBC-SHA",
    "TLS_SRP_SHA_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x1D},
    "TLS_SRP_SHA_WITH_AES_128_CBC_SHA",
    "SRP-AES-128-CBC-SHA",
    "TLS_SRP_SHA_AES_128_CBC_SHA1"},
  { {0xC0, 0x20},
    "TLS_SRP_SHA_WITH_AES_256_CBC_SHA",
    "SRP-AES-256-CBC-SHA",
    "TLS_SRP_SHA_AES_256_CBC_SHA1"},
  { {0xC0, 0x1C},
    "TLS_SRP_SHA_DSS_WITH_3DES_EDE_CBC_SHA",
     NULL,
    "TLS_SRP_SHA_DSS_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x1B},
    "TLS_SRP_SHA_RSA_WITH_3DES_EDE_CBC_SHA",
    "SRP-RSA-3DES-EDE-CBC-SHA",
    "TLS_SRP_SHA_RSA_3DES_EDE_CBC_SHA1"},
  { {0xC0, 0x1F},
    "TLS_SRP_SHA_DSS_WITH_AES_128_CBC_SHA",
     NULL,
    "TLS_SRP_SHA_DSS_AES_128_CBC_SHA1"},
  { {0xC0, 0x1E},
    "TLS_SRP_SHA_RSA_WITH_AES_128_CBC_SHA",
    "SRP-RSA-AES-128-CBC-SHA",
    "TLS_SRP_SHA_RSA_AES_128_CBC_SHA1"},
  { {0xC0, 0x22},
    "TLS_SRP_SHA_DSS_WITH_AES_256_CBC_SHA",
     NULL,
    "TLS_SRP_SHA_DSS_AES_256_CBC_SHA1"},
  { {0xC0, 0x21},
    "TLS_SRP_SHA_RSA_WITH_AES_256_CBC_SHA",
    "SRP-RSA-AES-256-CBC-SHA",
    "TLS_SRP_SHA_RSA_AES_256_CBC_SHA1"},
  { {0x00, 0x00},
    NULL,
    NULL,
    NULL}
};

/* free data assigned to the connection */
static void free_gnutls_data(struct st_gnutls_data *data)
{
  if (data)
  {
    if (data->key)
      gnutls_privkey_deinit(data->key);
    gnutls_pcert_deinit(&data->cert);
    free(data);
  }
}

/* map the gnutls cipher suite (defined by key exchange algorithm, cipher
   and mac algorithm) to the corresponding OpenSSL cipher name */
static const char *openssl_cipher_name(gnutls_kx_algorithm_t kx,
                                       gnutls_cipher_algorithm_t cipher,
                                       gnutls_mac_algorithm_t mac)
{
  unsigned int i=0;
  const char *name= 0;
  unsigned char sid[2];
  gnutls_kx_algorithm_t lkx;
  gnutls_cipher_algorithm_t lcipher;
  gnutls_mac_algorithm_t lmac;

  while ((name= gnutls_cipher_suite_info(i++, (unsigned char *)&sid, &lkx, &lcipher, &lmac, NULL)))
  {
    if (lkx == kx &&
        lcipher == cipher &&
        lmac == mac)
    {
      i=0;
      while (tls_ciphers[i].iana_name)
      {
        if (!memcmp(tls_ciphers[i].sid, &sid, 2))
        {
          if (tls_ciphers[i].openssl_name)
            return tls_ciphers[i].openssl_name;
          if (tls_ciphers[i].gnutls_name)
            return tls_ciphers[i].gnutls_name;
          return tls_ciphers[i].iana_name;
        }
        i++;
      }
    }
  }
  return NULL;
}

/* get priority string for a given openssl cipher name */
static char *get_priority(const char *cipher_name, char *priority, size_t len)
{
  unsigned int i= 0;
  while (tls_ciphers[i].iana_name)
  {
    if (strcmp(tls_ciphers[i].iana_name, cipher_name) == 0 ||
        (tls_ciphers[i].openssl_name &&
         strcmp(tls_ciphers[i].openssl_name, cipher_name) == 0) ||
        (tls_ciphers[i].gnutls_name &&
         strcmp(tls_ciphers[i].gnutls_name, cipher_name) == 0))
    {
      const char *name;
      gnutls_kx_algorithm_t kx;
      gnutls_cipher_algorithm_t cipher;
      gnutls_mac_algorithm_t mac;
      gnutls_protocol_t min_version;
      unsigned j= 0;

      if (!tls_ciphers[i].gnutls_name)
        return NULL;

      while ((name= gnutls_cipher_suite_info(j++, NULL, &kx, &cipher,
                                             &mac, &min_version)))
      {
        if (!strcmp(name, tls_ciphers[i].gnutls_name))
        {
          snprintf(priority, len - 1, ":+%s:+%s:+%s",
                   gnutls_cipher_get_name(cipher),
                   gnutls_mac_get_name(mac),
                   gnutls_kx_get_name(kx));
          return priority;
        }
      }
      return NULL;
    }
    i++;
  }
  return NULL;
}

#define MAX_SSL_ERR_LEN 100

static void ma_tls_set_error(MYSQL *mysql, void *ssl, int ssl_errno)
{
  char  ssl_error[MAX_SSL_ERR_LEN];
  const char *ssl_error_reason;
  MARIADB_PVIO *pvio= mysql->net.pvio;

  if (!ssl_errno)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Unknown SSL error");
    return;
  }

  /* give a more descriptive error message for alerts */
  if (ssl_errno == GNUTLS_E_FATAL_ALERT_RECEIVED)
  {
    gnutls_alert_description_t alert_desc;
    const char *alert_name;
    alert_desc= gnutls_alert_get((gnutls_session_t)ssl);
    alert_name= gnutls_alert_get_name(alert_desc);
    snprintf(ssl_error, MAX_SSL_ERR_LEN, "fatal alert received: %s",
             alert_name);
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 0, 
                   ssl_error);
    return;
  }

  if ((ssl_error_reason= gnutls_strerror(ssl_errno)))
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 0, 
                   ssl_error_reason);
    return;
  }
  snprintf(ssl_error, MAX_SSL_ERR_LEN, "SSL errno=%d", ssl_errno);
  pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 
                 ssl_error);
}


static void ma_tls_get_error(char *errmsg, size_t length, int ssl_errno)
{
  const char *ssl_error_reason;

  if (!ssl_errno)
  {
    strncpy(errmsg, "Unknown SSL error", length);
    return;
  }
  if ((ssl_error_reason= gnutls_strerror(ssl_errno)))
  {
    strncpy(errmsg, ssl_error_reason, length);
    return;
  }
  snprintf(errmsg, length, "SSL errno=%d", ssl_errno);
}

/*
  Initializes SSL and allocate global
  context SSL_context

  SYNOPSIS
    my_gnutls_start
      mysql        connection handle

  RETURN VALUES
    0  success
    1  error
*/
int ma_tls_start(char *errmsg, size_t errmsg_len)
{
  int rc= 0;

  if (ma_tls_initialized)
    return 0;

  pthread_mutex_init(&LOCK_gnutls_config,MY_MUTEX_INIT_FAST);
  pthread_mutex_lock(&LOCK_gnutls_config);

  if ((rc= gnutls_global_init()) != GNUTLS_E_SUCCESS)
  {
    ma_tls_get_error(errmsg, errmsg_len, rc);
    goto end;
  }
  ma_tls_initialized= TRUE;
end:
  pthread_mutex_unlock(&LOCK_gnutls_config);
  return rc;
}

/*
   Release SSL and free resources
   Will be automatically executed by 
   mysql_server_end() function

   SYNOPSIS
     my_gnutls_end()
       void

   RETURN VALUES
     void
*/
void ma_tls_end()
{
  if (ma_tls_initialized)
  {
    pthread_mutex_lock(&LOCK_gnutls_config);
    if (mariadb_deinitialize_ssl)
      gnutls_global_deinit();
    ma_tls_initialized= FALSE;
    pthread_mutex_unlock(&LOCK_gnutls_config);
    pthread_mutex_destroy(&LOCK_gnutls_config);
  }
  return;
}

static int ma_gnutls_set_ciphers(gnutls_session_t ssl, char *cipher_str)
{
  const char *err;
  char *token;
#define PRIO_SIZE 1024  
  char prio[PRIO_SIZE];

  if (!cipher_str)
    return gnutls_priority_set_direct(ssl, "NORMAL", &err);

  token= strtok(cipher_str, ":");

  strcpy(prio, "NONE:+VERS-TLS-ALL:+SIGN-ALL:+COMP-NULL");

  while (token)
  {
    char priority[1024];
    char *p= get_priority(token, priority, 1024);
    if (p)
      strncat(prio, p, PRIO_SIZE - strlen(prio) - 1);
    token = strtok(NULL, ":");
  }
  return gnutls_priority_set_direct(ssl, prio , &err);
}

static int ma_tls_load_cert(const char *filename,
                            enum ma_pem_type type,
                            const char *password,
                            void *ptr)
{
  gnutls_datum_t data;
  int rc;

  data.data= 0;

  if ((rc= gnutls_load_file(filename, &data)) < 0)
    goto error;

  switch(type) {
  case MA_TLS_PEM_CERT:
  {
    gnutls_x509_crt_t cert;
    if ((rc= gnutls_x509_crt_init(&cert)) < 0)
      goto error;
    if ((rc= gnutls_x509_crt_import(cert, &data, GNUTLS_X509_FMT_PEM)))
    {
      gnutls_x509_crt_deinit(cert);
      goto error;
    }
    *((gnutls_x509_crt_t *)ptr)= cert;
    return 0;
  }
  case MA_TLS_PEM_KEY:
  {
    gnutls_x509_privkey_t key;
    if ((rc= gnutls_x509_privkey_init(&key)) < 0)
      goto error;
    if ((rc= gnutls_x509_privkey_import2(key, &data, 
                                         GNUTLS_X509_FMT_PEM,
                                         password, 0)) < 0)
    {
      gnutls_x509_privkey_deinit(key);
      goto error;
    }
    *((gnutls_x509_privkey_t *)ptr)= key;
  }
  default:
    break;
  }
error:
  if (data.data)
    gnutls_free(data.data);
  return rc;
}

static int ma_tls_set_certs(MYSQL *mysql,
                            gnutls_certificate_credentials_t ctx)
{
  int  ssl_error= 0;
  gnutls_x509_privkey_t key= 0;
  gnutls_x509_crt_t cert= 0;

  if (mysql->options.ssl_ca)
  {

    ssl_error= gnutls_certificate_set_x509_trust_file(ctx,
                                                      mysql->options.ssl_ca,
                                                      GNUTLS_X509_FMT_PEM);
    if (ssl_error < 0)
      goto error;
  }
  gnutls_certificate_set_verify_function(ctx,
                                         my_verify_callback);

  if (mysql->options.ssl_key || mysql->options.ssl_cert)
  {
    char *keyfile= mysql->options.ssl_key;
    char *certfile= mysql->options.ssl_cert;
    unsigned char key_id1[65], key_id2[65];
    size_t key_id1_len, key_id2_len;

    if (!certfile)
      certfile= keyfile;
    else if (!keyfile)
      keyfile= certfile;

    if ((ssl_error= ma_tls_load_cert(keyfile, MA_TLS_PEM_KEY,
                                     mysql->options.extension ?
                                     mysql->options.extension->tls_pw : NULL,
                                     &key)) < 0)
      goto error;
    if ((ssl_error= ma_tls_load_cert(certfile, MA_TLS_PEM_CERT,
                                     NULL, &cert)) < 0)
      goto error;

    /* check if private key corresponds to certificate */
    key_id1_len= key_id2_len= sizeof(key_id1);
    if ((ssl_error= gnutls_x509_crt_get_key_id(cert, 0, 
                                               key_id1, &key_id1_len)) < 0 ||
        (ssl_error= gnutls_x509_privkey_get_key_id(key, 0, 
                                                   key_id2, &key_id2_len)) < 0)
      goto error;

    if (key_id1_len != key_id2_len ||
        memcmp(key_id1, key_id2, key_id1_len) != 0)
    {
      ssl_error= GNUTLS_E_CERTIFICATE_KEY_MISMATCH;
      goto error;
    }

    /* load cert/key into context */
    if ((ssl_error= gnutls_certificate_set_x509_key(ctx,
                                                    &cert,
                                                    1,
                                                    key)) < 0)
      goto error;
  }


  return ssl_error;
error:
  if (key)
    gnutls_x509_privkey_deinit(key);
  if (cert)
    gnutls_x509_crt_deinit(cert);
  return ssl_error;
}

void *ma_tls_init(MYSQL *mysql)
{
  gnutls_session_t ssl= NULL;
  gnutls_certificate_credentials_t ctx;
  int ssl_error= 0;
  struct st_gnutls_data *data= NULL;

  pthread_mutex_lock(&LOCK_gnutls_config);

  if (gnutls_certificate_allocate_credentials(&ctx) != GNUTLS_E_SUCCESS)
    goto error;

  if ((ssl_error= ma_tls_set_certs(mysql, ctx)) < 0)
    goto error;

  if ((ssl_error = gnutls_init(&ssl, GNUTLS_CLIENT & GNUTLS_NONBLOCK)) < 0)
    goto error;

  if (!(data= (struct st_gnutls_data *)calloc(1, sizeof(struct st_gnutls_data))))
    goto error;

  data->mysql= mysql;
  gnutls_session_set_ptr(ssl, (void *)data);
  /*
  gnutls_certificate_set_retrieve_function2(GNUTLS_xcred, client_cert_callback);
 */
  ssl_error= ma_gnutls_set_ciphers(ssl, mysql->options.ssl_cipher);
  if (ssl_error < 0)
    goto error;

  /* we don't load private key and cert by default - if the server requests
     a client certificate we will send it via callback function */
  if ((ssl_error= gnutls_credentials_set(ssl, GNUTLS_CRD_CERTIFICATE, ctx)) < 0)
    goto error;
  
  pthread_mutex_unlock(&LOCK_gnutls_config);
  return (void *)ssl;
error:
  free_gnutls_data(data);
  ma_tls_set_error(mysql, ssl, ssl_error);
  if (ssl)
    gnutls_deinit(ssl);
  pthread_mutex_unlock(&LOCK_gnutls_config);
  return NULL;
}

ssize_t ma_tls_push(gnutls_transport_ptr_t ptr, const void* data, size_t len)
{
  MARIADB_PVIO *pvio= (MARIADB_PVIO *)ptr;
  ssize_t rc= pvio->methods->write(pvio, data, len);
  return rc;
}

ssize_t ma_tls_pull(gnutls_transport_ptr_t ptr, void* data, size_t len)
{
  MARIADB_PVIO *pvio= (MARIADB_PVIO *)ptr;
  ssize_t rc= pvio->methods->read(pvio, data, len);
  return rc;
}

static int ma_tls_pull_timeout(gnutls_transport_ptr_t ptr, unsigned int ms)
{
  MARIADB_PVIO *pvio= (MARIADB_PVIO *)ptr;
  return pvio->methods->wait_io_or_timeout(pvio, 0, ms);
}

my_bool ma_tls_connect(MARIADB_TLS *ctls)
{
  gnutls_session_t ssl = (gnutls_session_t)ctls->ssl;
  my_bool blocking;
  MYSQL *mysql;
  MARIADB_PVIO *pvio;
  int ret;
  struct st_gnutls_data *data;
  data= (struct st_gnutls_data *)gnutls_session_get_ptr(ssl);
  mysql= data->mysql;

  if (!mysql)
    return 1;

  pvio= mysql->net.pvio;

  /* Set socket to blocking if not already set */
  if (!(blocking= pvio->methods->is_blocking(pvio)))
    pvio->methods->blocking(pvio, TRUE, 0);

  /* we don't use GnuTLS read/write functions */
  gnutls_transport_set_ptr(ssl, pvio);
  gnutls_transport_set_push_function(ssl, ma_tls_push);
  gnutls_transport_set_pull_function(ssl, ma_tls_pull);
  gnutls_transport_set_pull_timeout_function(ssl, ma_tls_pull_timeout);
  gnutls_handshake_set_timeout(ssl, pvio->timeout[PVIO_CONNECT_TIMEOUT]);

  do {
    ret = gnutls_handshake(ssl);
  } while (ret < 0 && gnutls_error_is_fatal(ret) == 0);

  if (ret < 0)
  {
    ma_tls_set_error(mysql, ssl, ret);
    /* restore blocking mode */
    gnutls_deinit((gnutls_session_t )ctls->ssl);
    free_gnutls_data(data);
    ctls->ssl= NULL;
    if (!blocking)
      pvio->methods->blocking(pvio, FALSE, 0);
    return 1;
  }
  ctls->ssl= (void *)ssl;
  return 0;
}

ssize_t ma_tls_read(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{
  return gnutls_record_recv((gnutls_session_t )ctls->ssl, (void *)buffer, length);
}

ssize_t ma_tls_write(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{ 
  return gnutls_record_send((gnutls_session_t )ctls->ssl, (void *)buffer, length);
}

my_bool ma_tls_close(MARIADB_TLS *ctls)
{
  if (ctls->ssl)
  {
    gnutls_certificate_credentials_t ctx;
    struct st_gnutls_data *data=
      (struct st_gnutls_data *)gnutls_session_get_ptr(ctls->ssl);
    /* this would be the correct way, however can't dectect afterwards
       if the socket is closed or not, so we don't send encrypted 
       finish alert.
    rc= gnutls_bye((gnutls_session_t )ctls->ssl, GNUTLS_SHUT_WR);
    */
    free_gnutls_data(data);
    gnutls_credentials_get(ctls->ssl, GNUTLS_CRD_CERTIFICATE, (void **)&ctx);
    gnutls_certificate_free_keys(ctx);
    gnutls_certificate_free_cas(ctx);
    gnutls_certificate_free_crls(ctx);
    gnutls_certificate_free_ca_names(ctx);
    gnutls_certificate_free_credentials(ctx);
    gnutls_deinit((gnutls_session_t )ctls->ssl);
    ctls->ssl= NULL;
  }
  return 0;
}

int ma_tls_verify_server_cert(MARIADB_TLS *ctls __attribute__((unused)))
{
  /* server verification is already handled before */
  return 0;
}

const char *ma_tls_get_cipher(MARIADB_TLS *ctls)
{
  gnutls_kx_algorithm_t kx;
  gnutls_cipher_algorithm_t cipher;
  gnutls_mac_algorithm_t mac;

  if (!ctls || !ctls->ssl)
    return NULL;

  mac= gnutls_mac_get((gnutls_session_t)ctls->ssl);
  cipher= gnutls_cipher_get((gnutls_session_t)ctls->ssl);
  kx= gnutls_kx_get((gnutls_session_t)ctls->ssl);
  return openssl_cipher_name(kx, cipher, mac);
}

static int my_verify_callback(gnutls_session_t ssl)
{
  unsigned int status= 0;
  const gnutls_datum_t *cert_list;
  unsigned int cert_list_size;
  struct st_gnutls_data *data= (struct st_gnutls_data *)gnutls_session_get_ptr(ssl);
  MYSQL *mysql;
  MARIADB_PVIO *pvio;
  
  gnutls_x509_crt_t cert;
  const char *hostname;

  mysql= data->mysql;
  pvio= mysql->net.pvio;

  /* read hostname */
  hostname = mysql->host;

  /* This verification function uses the trusted CAs in the credentials
   * structure. So you must have installed one or more CA certificates.
   */
  if ((mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT)  &&
        gnutls_certificate_verify_peers2 (ssl, &status) < 0)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "CA verification failed");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }

  if (status & GNUTLS_CERT_INVALID)
  {
    return GNUTLS_E_CERTIFICATE_ERROR;
  }
  /* Up to here the process is the same for X.509 certificates and
   * OpenPGP keys. From now on X.509 certificates are assumed. This can
   * be easily extended to work with openpgp keys as well.
   */
  if (gnutls_certificate_type_get (ssl) != GNUTLS_CRT_X509)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Expected X509 certificate");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }
  if (gnutls_x509_crt_init (&cert) < 0)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Error during certificate initialization");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }
  cert_list = gnutls_certificate_get_peers (ssl, &cert_list_size);
  if (cert_list == NULL)
  {
    gnutls_x509_crt_deinit (cert);
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "No certificate found");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }
  if (gnutls_x509_crt_import (cert, &cert_list[0], GNUTLS_X509_FMT_DER) < 0)
  {
    gnutls_x509_crt_deinit (cert);
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Unknown SSL error");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }

  if ((mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT) &&
      !gnutls_x509_crt_check_hostname (cert, hostname))
  {
    gnutls_x509_crt_deinit (cert);
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Hostname in certificate doesn't match");
    return GNUTLS_E_CERTIFICATE_ERROR;
  }
  gnutls_x509_crt_deinit (cert);
  /* notify gnutls to continue handshake normally */

  CLEAR_CLIENT_ERROR(mysql);
  return 0;
}

unsigned int ma_tls_get_finger_print(MARIADB_TLS *ctls, char *fp, unsigned int len)
{
  MYSQL *mysql;
  size_t fp_len= len;
  const gnutls_datum_t *cert_list;
  unsigned int cert_list_size;
  struct st_gnutls_data *data;

  if (!ctls || !ctls->ssl)
    return 0;

  data= (struct st_gnutls_data *)gnutls_session_get_ptr(ctls->ssl);
  mysql= (MYSQL *)data->mysql;

  cert_list = gnutls_certificate_get_peers (ctls->ssl, &cert_list_size);
  if (cert_list == NULL)
  {
    my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                        ER(CR_SSL_CONNECTION_ERROR), 
                        "Unable to get server certificate");
    return 0;
  }

  if (gnutls_fingerprint(GNUTLS_DIG_SHA1, &cert_list[0], fp, &fp_len) == 0)
    return fp_len;
  else
  {
    my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                        ER(CR_SSL_CONNECTION_ERROR), 
                        "Finger print buffer too small");
    return 0;
  }
}

my_bool ma_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version)
{
  if (!ctls || !ctls->ssl)
    return 1;

  version->iversion= gnutls_protocol_get_version(ctls->ssl);
  version->cversion= (char *)gnutls_protocol_get_name(version->iversion);
  return 0;  
}
#endif /* HAVE_GNUTLS */
