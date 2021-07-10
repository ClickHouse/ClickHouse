/* LTO IL compression streams.

   Copyright (C) 2009-2018 Free Software Foundation, Inc.
   Contributed by Simon Baldwin <simonb@google.com>

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_LTO_COMPRESS_H
#define GCC_LTO_COMPRESS_H

struct lto_compression_stream;

/* In lto-compress.c.  */
extern struct lto_compression_stream
  *lto_start_compression (void (*callback) (const char *, unsigned, void *),
			  void *opaque);
extern void lto_compress_block (struct lto_compression_stream *stream,
				const char *base, size_t num_chars);
extern void lto_end_compression (struct lto_compression_stream *stream);

extern struct lto_compression_stream
  *lto_start_uncompression (void (*callback) (const char *, unsigned, void *),
			    void *opaque);
extern void lto_uncompress_block (struct lto_compression_stream *stream,
				  const char *base, size_t num_chars);
extern void lto_end_uncompression (struct lto_compression_stream *stream);

#endif /* GCC_LTO_COMPRESS_H  */
