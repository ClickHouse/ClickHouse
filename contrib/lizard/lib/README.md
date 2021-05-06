Lizard - Library Files
================================

The __lib__ directory contains several directories.
Depending on target use case, it's enough to include only files from relevant directories.


#### API

Lizard stable API is exposed within [lizard_compress.h](lizard_compress.h) and [lizard_decompress.h](lizard_decompress.h),
at the root of `lib` directory.


#### Compatibility issues

The raw Lizard block compression format is detailed within [lizard_Block_format].
To compress an arbitrarily long file or data stream, multiple blocks are required.
Organizing these blocks and providing a common header format to handle their content
is the purpose of the Frame format, defined in [lizard_Frame_format].
`lizard` command line utility produces files or streams compatible with the Frame format.
(_Advanced stuff_ : It's possible to hide xxhash symbols into a local namespace.
This is what `liblizard` does, to avoid symbol duplication
in case a user program would link to several libraries containing xxhash symbols.)

[lizard_Block_format]: ../doc/lizard_Block_format.md
[lizard_Frame_format]: ../doc/lizard_Frame_format.md


#### Various Lizard builds

Files `lizard_common.h`, `lizard_compress*`, `lizard_parser_*.h`, `lizard_decompress*`, and `entropy\mem.h` are required in all circumstances.

To compile:
- Lizard_raw only with levels 10...29 : use the `-DLIZARD_NO_HUFFMAN` compiler flag
- Lizard_raw with levels 10...49 : include also all files from `entropy` directory
- Lizard_frame with levels 10...49 : `lizard_frame*` and all files from `entropy` and `xxhash` directories


#### Advanced API 

A more complex `lizard_frame_static.h` is also provided.
It contains definitions which are not guaranteed to remain stable within future versions.
It must be used with static linking ***only***.


#### Using MinGW+MSYS to create DLL

DLL can be created using MinGW+MSYS with the `make liblizard` command.
This command creates `dll\liblizard.dll` and the import library `dll\liblizard.lib`.
The import library is only required with Visual C++.
The header files `lizard.h`, `lizardhc.h`, `lizard_frame.h` and the dynamic library
`dll\liblizard.dll` are required to compile a project using gcc/MinGW.
The dynamic library has to be added to linking options.
It means that if a project that uses Lizard consists of a single `test-dll.c`
file it should be compiled with `liblizard.lib`. For example:
```
    gcc $(CFLAGS) -Iinclude/ test-dll.c -o test-dll dll\liblizard.dll
```
The compiled executable will require Lizard DLL which is available at `dll\liblizard.dll`. 


#### Miscellaneous 

Other files present in the directory are not source code. There are :

 - LICENSE : contains the BSD license text
 - Makefile : script to compile or install lizard library (static or dynamic)
 - liblizard.pc.in : for pkg-config (make install)
 - README.md : this file


#### License 

All source material within __lib__ directory are BSD 2-Clause licensed.
See [LICENSE](LICENSE) for details.
The license is also repeated at the top of each source file.
