/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
                 2016 MariaDB Corporation AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA */

/* Written by Sinisa Milivojevic <sinisa@coresinc.com> */

#include <ma_global.h>
#ifdef HAVE_COMPRESS
#include <ma_sys.h>
#include <ma_string.h>
#include <zlib.h>

/*
** This replaces the packet with a compressed packet
** Returns 1 on error
** *complen is 0 if the packet wasn't compressed
*/

my_bool _mariadb_compress(unsigned char *packet, size_t *len, size_t *complen)
{
  if (*len < MIN_COMPRESS_LENGTH)
    *complen=0;
  else
  {
    unsigned char *compbuf=_mariadb_compress_alloc(packet,len,complen);
    if (!compbuf)
      return *complen ? 0 : 1;
    memcpy(packet,compbuf,*len);
    free(compbuf);
  }
  return 0;
}


unsigned char *_mariadb_compress_alloc(const unsigned char *packet, size_t *len, size_t *complen)
{
  unsigned char *compbuf;
  *complen =  *len * 120 / 100 + 12;
  if (!(compbuf = (unsigned char *) malloc(*complen)))
    return 0;					/* Not enough memory */
  if (compress((Bytef*) compbuf,(ulong *) complen, (Bytef*) packet,
	       (uLong) *len ) != Z_OK)
  {
    free(compbuf);
    return 0;
  }
  if (*complen >= *len)
  {
    *complen=0;
    free(compbuf);
    return 0;
  }
  swap(size_t,*len,*complen);			/* *len is now packet length */
  return compbuf;
}

my_bool _mariadb_uncompress (unsigned char *packet, size_t *len, size_t *complen)
{
  if (*complen)					/* If compressed */
  {
    unsigned char *compbuf = (unsigned char *) malloc (*complen);
    if (!compbuf)
      return 1;					/* Not enough memory */
    if (uncompress((Bytef*) compbuf, (uLongf *)complen, (Bytef*) packet, (uLongf)*len) != Z_OK)
    {						/* Probably wrong packet */
      free(compbuf);
      return 1;
    }
    *len = *complen;
    memcpy(packet,compbuf,*len);
    free(compbuf);
  }
  else *complen= *len;
  return 0;
}
#endif /* HAVE_COMPRESS */
