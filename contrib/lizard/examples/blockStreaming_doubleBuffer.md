# Lizard streaming API Example : Double Buffer
by *Takayuki Matsuoka*

`blockStreaming_doubleBuffer.c` is Lizard Straming API example which implements double buffer (de)compression.

Please note :

 - Firstly, read "Lizard Streaming API Basics".
 - This is relatively advanced application example.
 - Output file is not compatible with lizard_frame and platform dependent.


## What's the point of this example ?

 - Handle huge file in small amount of memory
 - Always better compression ratio than Block API
 - Uniform block size


## How the compression works

First of all, allocate "Double Buffer" for input and Lizard compressed data buffer for output.
Double buffer has two pages, "first" page (Page#1) and "second" page (Page#2).

```
        Double Buffer

      Page#1    Page#2
    +---------+---------+
    | Block#1 |         |
    +----+----+---------+
         |
         v
      {Out#1}


      Prefix Dependency
         +---------+
         |         |
         v         |
    +---------+----+----+
    | Block#1 | Block#2 |
    +---------+----+----+
                   |
                   v
                {Out#2}


   External Dictionary Mode
         +---------+
         |         |
         |         v
    +----+----+---------+
    | Block#3 | Block#2 |
    +----+----+---------+
         |
         v
      {Out#3}


      Prefix Dependency
         +---------+
         |         |
         v         |
    +---------+----+----+
    | Block#3 | Block#4 |
    +---------+----+----+
                   |
                   v
                {Out#4}
```

Next, read first block to double buffer's first page. And compress it by `Lizard_compress_continue()`.
For the first time, Lizard doesn't know any previous dependencies,
so it just compress the line without dependencies and generates compressed block {Out#1} to Lizard compressed data buffer.
After that, write {Out#1} to the file.

Next, read second block to double buffer's second page. And compress it.
In this time, Lizard can use dependency to Block#1 to improve compression ratio.
This dependency is called "Prefix mode".

Next, read third block to double buffer's *first* page. And compress it.
Also this time, Lizard can use dependency to Block#2.
This dependency is called "External Dictonaly mode".

Continue these procedure to the end of the file.


## How the decompression works

Decompression will do reverse order.

 - Read first compressed block.
 - Decompress it to the first page and write that page to the file.
 - Read second compressed block.
 - Decompress it to the second page and write that page to the file.
 - Read third compressed block.
 - Decompress it to the *first* page and write that page to the file.

Continue these procedure to the end of the compressed file.
