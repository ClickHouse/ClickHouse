Command Line Interface for LZ4 library
============================================

Command Line Interface (CLI) can be created using the `make` command without any additional parameters.
There are also multiple targets that create different variations of CLI:
- `lizard` : default CLI, with a command line syntax close to gzip
- `lizardc32` : Same as `lizard`, but forced to compile in 32-bits mode


#### Aggregation of parameters
CLI supports aggregation of parameters i.e. `-b1`, `-e18`, and `-i1` can be joined into `-b1e18i1`.



#### Benchmark in Command Line Interface
CLI includes in-memory compression benchmark module for lizard.
The benchmark is conducted using a given filename.
The file is read into memory.
It makes benchmark more precise as it eliminates I/O overhead.

The benchmark measures ratio, compressed size, compression and decompression speed.
One can select compression levels starting from `-b` and ending with `-e`.
The `-i` parameter selects a number of seconds used for each of tested levels.



#### Usage of Command Line Interface
The full list of commands can be obtained with `-h` or `-H` parameter:
```
Usage :
      lizard [arg] [input] [output]

input   : a filename
          with no FILE, or when FILE is - or stdin, read standard input
Arguments :
 -10...-19 : compression method fastLZ4 = 16-bit bytewise codewords
             higher number == more compression but slower
 -20...-29 : compression method LIZv1 = 24-bit bytewise codewords
 -30...-39 : compression method fastLZ4 + Huffman
 -40...-49 : compression method LIZv1 + Huffman
 -d     : decompression (default for .liz extension)
 -z     : force compression
 -f     : overwrite output without prompting
--rm    : remove source file(s) after successful de/compression
 -h/-H  : display help/long help and exit

Advanced arguments :
 -V     : display Version number and exit
 -v     : verbose mode
 -q     : suppress warnings; specify twice to suppress errors too
 -c     : force write to standard output, even if it is the console
 -t     : test compressed file integrity
 -m     : multiple input files (implies automatic output filenames)
 -r     : operate recursively on directories (sets also -m)
 -l     : compress using Legacy format (Linux kernel compression)
 -B#    : Block size [1-7] = 128KB, 256KB, 1MB, 4MB, 16MB, 64MB, 256MB (default : 4)
 -BD    : Block dependency (improve compression ratio)
--no-frame-crc : disable stream checksum (default:enabled)
--content-size : compressed frame includes original size (default:not present)
--[no-]sparse  : sparse mode (default:enabled on file, disabled on stdout)
Benchmark arguments :
 -b#    : benchmark file(s), using # compression level (default : 1)
 -e#    : test all compression levels from -bX to # (default : 1)
 -i#    : minimum evaluation time in seconds (default : 3s)
 -B#    : cut file into independent blocks of size # bytes [32+]
                      or predefined block size [1-7] (default: 4)
```

#### License

All files in this directory are licensed under GPL-v2.
See [COPYING](COPYING) for details.
The text of the license is also included at the top of each source file.
