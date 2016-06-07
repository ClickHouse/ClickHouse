## MetroHash: Faster, Better Hash Functions

MetroHash is a set of state-of-the-art hash functions for *non-cryptographic* use cases. They are notable for being algorithmically generated in addition to their exceptional performance. The set of published hash functions may be expanded in the future, having been selected from a very large set of hash functions that have been constructed this way.

* Fastest general-purpose functions for bulk hashing. 
* Fastest general-purpose functions for small, variable length keys. 
* Robust statistical bias profile, similar to the MD5 cryptographic hash.
* 64-bit, 128-bit, and 128-bit CRC variants currently available.
* Optimized for modern x86-64 microarchitectures.
* Elegant, compact, readable functions.
 
You can read more about the design and history [here](http://www.jandrewrogers.com/2015/05/27/metrohash/).

Six hash functions have been included in the initial release:

* 64-bit hash functions, "metrohash64_1" and "metrohash64_2"
* 128-bit hash functions, "metrohash128_1" and "metrohash128_2"
* 128-bit hash functions using CRC instructions, "metrohash128crc_1" and "metrohash128crc_2"
 
Hash functions in the same family are effectively statistically unique. In other words, if you need two hash functions for a bloom filter, you can use "metrohash64_1" and "metrohash64_2" in the same implementation without issue. An unbounded set of statistically unique functions can be generated in each family. The functions in this repo were generated specifically for public release.

The hash function generation software made no effort toward portability. While these hash functions should be easily portable to big-endian microarchitectures, they have not been tested on them and the performance optimization algorithms were not targeted at them. ARM64 microarchitectures might be a worthwhile hash function generation targets if I had the hardware.


