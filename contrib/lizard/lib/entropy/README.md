New Generation Entropy library
==============================

The __lib__ directory contains several files, but you don't necessarily want them all.
Here is a detailed list, to help you decide which one you need :


#### Compulsory files

These files are required in all circumstances :
- __error_public.h__ : error list as enum
- __error_private.h__ : error management
- __mem.h__ : low level memory access routines
- __bitstream.h__ : generic read/write bitstream common to all entropy codecs
- __entropy_common.c__ : common functions needed for both compression and decompression


#### Finite State Entropy

This is the base codec required by other ones.
It implements a tANS variant, similar to arithmetic in compression performance, but much faster. Compression and decompression can be compiled independently.
- __fse.h__ : exposes interfaces
- __fse_compress.c__ : implements compression codec
- __fse_decompress.c__ : implements decompression codec


#### FSE 16-bits symbols version

This codec is able to encode alphabets of size > 256, using 2 bytes per symbol. It requires the base FSE codec to compile properly. Compression and decompression are merged in the same file.
- __fseU16.c__ implements the codec, while __fseU16.h__ exposes its interfaces.


#### Huffman codec

This is the fast huffman codec. It requires the base FSE codec to compress its headers. Compression and decompression can be compiled independently.
- __huf.h__ : exposes interfaces.
- __huf_compress.c__ : implements compression codec
- __huf_decompress.c__ : implements decompression codec
