0.14.2
------
*February 12, 2018*
*   Improved chameleon decode speed
*   Added data hash checks and display option in benchmark
*   Now using makefiles as build system
*   Big endian support correctly implemented and tested
*   Improved continuous integration tests

0.14.1
------
*January 20, 2018*
*   Added MSVC support
*   Added continuous integration on travis and appveyor
*   Premake script improvement
*   Various codebase improvements

0.14.0
------
*January 16, 2018*
*   First stable version of DENSITY
*   Complete project reorganization and API rewrite
*   Many stability fixes and improvements
*   Fast revert to conditional copy for incompressible input
*   Custom dictionaries in API
*   Improvements in compression ratio and speed

0.12.5 beta
-----------
*June 20, 2015*

*   Added conditional main footer read/write
*   Improved teleport staging buffer management
*   Regression - a minimum buffer output size has to be ensured to avoid signature loss
*   Modified the minimum lookahead and the resulting minimum buffer size in the API
*   Lion : corrected a signature interception problem due to an increase in process unit size
*   Lion : corrected chunk count conditions for new block / mode marker detection
*   Lion : modified end of stream marker conditions
*   Stability fixes and improvements

0.12.4 beta
-----------
*May 25, 2015*

*   Removed remaining undefined behavior potential occurences
*   Implemented parallelizable decompressible output block header reads/writes (disabled by default)

0.12.3 beta
-----------
*May 20, 2015*

*   New lion algorithm, faster and more efficient
*   Compiler specific optimizations
*   Switched to premake 5 to benefit from link time optimizations
*   Various fixes and improvements

0.12.2 beta
-----------
*May 4, 2015*

*   Added an integrated in-memory benchmark
*   Better Windows compatibility
*   Fixed misaligned load/stores
*   Switched to the premake build system
*   Performance optimizations (pointers, branches, loops ...)
*   Various fixes and improvements

0.12.1 beta
-----------
*April 3, 2015*

*   Better unrolling readability and efficiency
*   Improved read speed of dictionary/predictions entries
*   Implemented case generators in cheetah to speed up decoding by using less branches
*   Added signatures interception in lion to cancel the need for large output buffers
*   Improved lion decode speed with specific form data access and use of ctz in form read
*   Enabled decompression to exact-sized buffer for all algorithms
*   Various fixes and improvements

0.12.0 beta
-----------
*March 24, 2015*

*   Added new lion kernel
*   Renamed kernel mandala to cheetah
*   Kernel chameleon and cheetah improvements in encoding/decoding speeds
*   Generic function macros to avoid code rewrite
*   Improved memory teleport IO flexibility and speed, bytes issued by memory teleport can now be partially read
*   Various fixes and improvements

0.11.3 beta
-----------
*February 5, 2015*

*   Added integrity check system
*   Corrected pointer usage and update on footer read/writes
*   Now freeing kernel state memory only when compression mode is not copy
*   Updated Makefiles
*   Improved memory teleport
*   Fixed sequencing problem after kernels request a new block

0.11.2 beta
-----------
*February 3, 2015*

*   Added an algorithms overview in README
*   Removed ssc references
*   Now initializing last hash to zero on mandala kernel inits
*   Reimplemented the buffer API
*   Various corrections and improvements

0.11.1 beta
-----------
*January 19, 2015*

*   Added a sharc benchmark in README
*   Stateless memory teleport
*   Improved event management and dispatching
*   Improved compression/decompression finishes
*   Improved streams API
*   Various bug fixes, robustness improvements

0.10.2 beta
-----------
*January 7, 2015*

*   Improved organization of compile-time switches and run-time options in the API
*   Removed method density_stream_decompress_utilities_get_header from the API, header info is now returned in the density_stream_decompress_init function
*   Corrected readme to reflect API changes

0.10.1 beta
-----------
*January 5, 2015*

*   Re-added mandala kernel
*   Corrected available bytes adjustment problem
*   Added missing restrict keywords
*   Cleaned unnecessary defines

0.10.0 beta
-----------
*January 2, 2015*

*   Complete stream API redesign to greatly improve flexibility
*   Only one supported algorithm for now : Chameleon

0.9.12 beta
-----------
*December 2, 2013*

*   Mandala kernel addition, replacing dual pass chameleon
*   Simplified, faster hash function
*   Fixed memory freeing issue during main encoding/decoding finish
*   Implemented no footer encode output type
*   Namespace migration, kernel structure reorganization
*   Corrected copy mode problem
*   Implemented efficiency checks and mode reversions
*   Corrected lack of main header parameters retrieval
*   Fixed stream not being properly ended when mode reversion occurred
*   Updated metadata computations

0.9.11 beta
-----------
*November 2, 2013*

*   First beta release of DENSITY, including all the compression code from SHARC in a standalone, BSD licensed library
*   Added copy mode (useful for enhancing data security via the density block checksums for example)
*   Makefile produces static and dynamic libraries
