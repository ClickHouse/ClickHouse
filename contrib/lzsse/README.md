# LZSSE
[LZSS](https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Storer%E2%80%93Szymanski) designed for a branchless SSE decompression implementation.

Three variants:
- LZSSE2, for high compression files with small literal runs.
- LZSSE4, for a more balanced mix of literals and matches.
- LZSSE8, for lower compression data with longer runs of matches.

All three variants have an optimal parser implementation, which uses a quite strong match finder (very similar to LzFind) combined with a Storer-Szymanski style parse. LZSSE4 and LZSSE8 have "fast" compressor implementations, which use a simple hash table based matching and a greedy parse.

Currently LZSSE8 is the recommended variant to use in the general case, as it generally performs well in most cases (and you have the option of both optimal parse and fast compression). LZSSE2 is recommended if you are only using text, especially heavily compressible text, but is slow/doesn't compress as well on less compressible data and binaries.

The code is approaching production readiness and LZSSE2 and LZSSE8 have received a reasonable amount of testing.

See these blog posts [An LZ Codec Designed for SSE Decompression](http://conorstokes.github.io/compression/2016/02/15/an-LZ-codec-designed-for-SSE-decompression) and [Compressor Improvements and LZSSE2 vs LZSSE8](http://conorstokes.github.io/compression/2016/02/24/compressor-improvements-and-lzsse2-vs-lzsse8) for a description of how the compression algorithm and implementation function. There are also benchmarks, but these may not be upto date (in particular the figures in the initial blog post no longer represent compression performance).
