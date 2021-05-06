Lizard v1.x Block Format Description
============================
Last revised: 2016-10-08
Authors : Yann Collet, Przemyslaw Skibinski


This specification is intended for developers
willing to produce Lizard-compatible compressed data blocks
using any programming language.

Lizard is an LZ77-type compressor with a fixed, byte-oriented encoding.
There is no framing layer as it is assumed to be handled by other parts of the system (see [Lizard Frame format]).
This design is assumed to favor simplicity and speed.
It helps later on for optimizations, compactness, and features.

This document describes only the block format,
not how the compressor nor decompressor actually work.
The correctness of the decompressor should not depend
on implementation details of the compressor, and vice versa.

[Lizard Frame format]: lizard_Frame_format.md


Division into blocks
--------------------

The input data is divided into blocks of maximum size LIZARD_BLOCK_SIZE (which is 128 KB). The subsequent blocks use the same sliding window and are dependent on previous blocks.
Our impementation of Lizard compressor divides input data into blocks of size of LIZARD_BLOCK_SIZE except the last one which usually will be smaller.
The output data is a single byte 'Compression_Level' and one or more blocks in the format described below.


Block header format
-----------------------

The block header is a single byte `Header_Byte` that is combination of following flags:

| Name                | Value |
| --------------------- | --- |
| LIZARD_FLAG_LITERALS     | 1   |
| LIZARD_FLAG_FLAGS        | 2   |
| LIZARD_FLAG_OFF16LEN     | 4   |
| LIZARD_FLAG_OFF24LEN     | 8   |
| LIZARD_FLAG_LEN          | 16  |
| LIZARD_FLAG_UNCOMPRESSED | 128 |

When `Header_Byte & LIZARD_FLAG_UNCOMPRESSED` is true then the block is followed by 3-byte `Uncompressed_length` and uncompressed data of given size.


Compressed block content
------------------------

When `Header_Byte & LIZARD_FLAG_UNCOMPRESSED` is false then compressed block contains of 5 streams:
- `Lengths_Stream` (compressed with Huffman if LIZARD_FLAG_LEN is set)
- `16-bit_Offsets_Stream` (compressed with Huffman if LIZARD_FLAG_OFF16LEN is set)
- `24-bit_Offsets_Stream` (compressed with Huffman if LIZARD_FLAG_OFF24LEN is set)
- `Tokens_Stream` (compressed with Huffman if LIZARD_FLAG_FLAGS is set)
- `Literals_Stream` (compressed with Huffman if LIZARD_FLAG_LITERALS is set)


Stream format
-------------
The single stream is either:
- if LIZARD_FLAG_XXX is not set: 3 byte `Stream_Length` followed by a given number bytes
- if LIZARD_FLAG_XXX is set: 3 byte `Original_Stream_Length`, 3 byte `Compressed_Stream_Length`, followed by a given number of Huffman compressed bytes


Lizard block decompression
-----------------------
At the beginning we have 5 streams and their sizes.
Decompressor should iterate through `Tokens_Stream`. Each token is 1-byte long and describes how to get data from other streams. 
If token points a stream that is already empty it means that data is corrupted.


Lizard token decompression
-----------------------
The token is a one byte. Token decribes:
- how many literals should be copied from `Literals_Stream`
- if offset should be read from `16-bit_Offsets_Stream` or `24-bit_Offsets_Stream`
- how many bytes are part of a match and should be copied from a sliding window

Lizard uses 4 types of tokens:
- [0_MMMM_LLL] - 3-bit literal length (0-7+), use offset from `16-bit_Offsets_Stream`, 4-bit match length (4-15+)
- [1_MMMM_LLL] - 3-bit literal length (0-7+), use last offset, 4-bit match length (0-15+)
- token 31     - no literal length, use offset from `24-bit_Offsets_Stream`, match length (47+)
- token 0-30   - no literal length, use offset from `24-bit_Offsets_Stream`, 31 match lengths (16-46)

Lizard uses different output codewords and is not compatible with LZ4. LZ4 output codewords are 3 byte long (24-bit) and look as follows:
- LLLL_MMMM OOOOOOOO OOOOOOOO - 16-bit offset, 4-bit match length, 4-bit literal length 


The format of `Lengths_Stream`
------------------------------
`Lengths_Stream` contains lenghts in the the following format:
- when 'First_Byte' is < 254 then lenght is equal 'First_Byte'
- when 'First_Byte' is 254 then lenght is equal to value of 2-bytes after 'First_Byte' i.e. 0-65536
- when 'First_Byte' is 255 then lenght is equal to value of 3-bytes after 'First_Byte' i.e. 0-16777215


[0_MMMM_LLL] and [1_MMMM_LLL] tokens
---------------------------------------
The length of literals to be copied from `Literals_Stream` depends on the literal length field (LLL) that uses 3 bits of the token.
Therefore each field ranges from 0 to 7.
If the value is 7, then the lenght is increased with a length taken from `Lengths_Stream`.

Example 1 : A literal length of 48 will be represented as :

  - 7  : value for the 3-bits LLL field
  - 41 : (=48-7) remaining length to reach 48 (in `Lengths_Stream`)

Example 2 : A literal length of 280 for will be represented as :

  - 7   : value for the 3-bits LLL field
  - 254 : informs that remaining length (=280-7) must be represented as 2-bytes (in `Lengths_Stream`)
  - 273 : (=280-7) encoded as 2-bytes (in `Lengths_Stream`)

Example 3 : A literal length of 7 for will be represented as :

  - 7  : value for the 3-bits LLL field
  - 0  : (=7-7) yes, the zero must be output (in `Lengths_Stream`)

After copying 0 or more literals from `Literals_Stream` we can prepare the match copy operation which depends on a offset and a match length.
The flag "0" informs that decoder should use the last encoded offset.
The flag "1" informs that the offset is a 2 bytes value (16-bit), in little endian format and should be taken from `16-bit_Offsets_Stream`.

The match length depends on the match length field (MMMM) that uses 4 bits of the token.
Therefore each field ranges from 0 to 15. Values from 0-3 are forbidden with offset taken from `16-bit_Offsets_Stream`.
If the value is 15, then the lenght is increased with a length taken from `Lengths_Stream`.

With the offset and the match length,
the decoder can now proceed to copy the data from the already decoded buffer.


Lizard block epilogue
------------------
When all tokens are read from `Tokens_Stream` and interpreted all remaining streams should also be empty.
Otherwise, it means that the data is corrupted. The only exception is `Literals_Stream` that should have at least 16 remaining literals what
allows fast memory copy operations. The remaining literals up to the end of `Literals_Stream` should be appended to the output data.


Tokens 0-31
-----------
The offset is a 3 bytes value (24-bit), in little endian format and should be taken from `24-bit_Offsets_Stream`.
The offset represents the position of the match to be copied from.
1 means "current position - 1 byte".
The maximum offset value is (1<<24)-1, 1<<24 cannot be coded.
Note that 0 is an invalid value, not used. 

The 'Token_Value' ranges from 0 to 31.
The match length is equal to 'Token_Value + 16 that is from 16 to 47.
If match length is 47, the lenght is increased with a length taken from `Lengths_Stream`.


Parsing restrictions
-----------------------
There are specific parsing rules to respect in order to remain compatible
with assumptions made by the decoder :

1. The last 16 bytes are always literals what allows fast memory copy operations.
2. The last match must start at least 20 bytes before end of block.   
   Consequently, a block with less than 20 bytes cannot be compressed.

These rules are in place to ensure that the decoder
will never read beyond the input buffer, nor write beyond the output buffer.

Note that the last sequence is also incomplete,
and stops right after literals.


Additional notes
-----------------------
There is no assumption nor limits to the way the compressor
searches and selects matches within the source data block.
It could be a fast scan, a multi-probe, a full search using BST,
standard hash chains or MMC, well whatever.

Advanced parsing strategies can also be implemented, such as lazy match,
or full optimal parsing.

All these trade-off offer distinctive speed/memory/compression advantages.
Whatever the method used by the compressor, its result will be decodable
by any Lizard decoder if it follows the format specification described above.
