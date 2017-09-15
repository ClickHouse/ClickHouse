Build
=====

with gcc:
> gcc -O3 -msse2 FastMemcpy.c -o FastMemcpy

with msvc:
> cl -nologo -O2 FastMemcpy.c

Features
========

* 50% speedup in avg. vs traditional memcpy in msvc 2012 or gcc 4.9
* small size copy optimized with jump table
* medium size copy optimized with sse2 vector copy
* huge size copy optimized with cache prefetch & movntdq

Reference
=========

[Using Block Prefetch for Optimized Memory Performance](http://files.rsdn.ru/23380/AMD_block_prefetch_paper.pdf)

The artical only focused on aligned huge memory copy. You need handle other conditions by your self.


Results
=======

```
result: gcc4.9 (msvc 2012 got a similar result):

benchmark(size=32 bytes, times=16777216):
result(dst aligned, src aligned): memcpy_fast=81ms memcpy=281 ms
result(dst aligned, src unalign): memcpy_fast=88ms memcpy=254 ms
result(dst unalign, src aligned): memcpy_fast=87ms memcpy=245 ms
result(dst unalign, src unalign): memcpy_fast=81ms memcpy=258 ms

benchmark(size=64 bytes, times=16777216):
result(dst aligned, src aligned): memcpy_fast=91ms memcpy=364 ms
result(dst aligned, src unalign): memcpy_fast=95ms memcpy=336 ms
result(dst unalign, src aligned): memcpy_fast=96ms memcpy=353 ms
result(dst unalign, src unalign): memcpy_fast=99ms memcpy=346 ms

benchmark(size=512 bytes, times=8388608):
result(dst aligned, src aligned): memcpy_fast=124ms memcpy=242 ms
result(dst aligned, src unalign): memcpy_fast=166ms memcpy=555 ms
result(dst unalign, src aligned): memcpy_fast=168ms memcpy=602 ms
result(dst unalign, src unalign): memcpy_fast=174ms memcpy=614 ms

benchmark(size=1024 bytes, times=4194304):
result(dst aligned, src aligned): memcpy_fast=119ms memcpy=171 ms
result(dst aligned, src unalign): memcpy_fast=182ms memcpy=442 ms
result(dst unalign, src aligned): memcpy_fast=163ms memcpy=466 ms
result(dst unalign, src unalign): memcpy_fast=168ms memcpy=472 ms

benchmark(size=4096 bytes, times=524288):
result(dst aligned, src aligned): memcpy_fast=68ms memcpy=82 ms
result(dst aligned, src unalign): memcpy_fast=94ms memcpy=226 ms
result(dst unalign, src aligned): memcpy_fast=134ms memcpy=216 ms
result(dst unalign, src unalign): memcpy_fast=84ms memcpy=188 ms

benchmark(size=8192 bytes, times=262144):
result(dst aligned, src aligned): memcpy_fast=55ms memcpy=70 ms
result(dst aligned, src unalign): memcpy_fast=75ms memcpy=192 ms
result(dst unalign, src aligned): memcpy_fast=79ms memcpy=223 ms
result(dst unalign, src unalign): memcpy_fast=91ms memcpy=219 ms

benchmark(size=1048576 bytes, times=2048):
result(dst aligned, src aligned): memcpy_fast=181ms memcpy=165 ms
result(dst aligned, src unalign): memcpy_fast=192ms memcpy=303 ms
result(dst unalign, src aligned): memcpy_fast=218ms memcpy=310 ms
result(dst unalign, src unalign): memcpy_fast=183ms memcpy=307 ms

benchmark(size=4194304 bytes, times=512):
result(dst aligned, src aligned): memcpy_fast=263ms memcpy=398 ms
result(dst aligned, src unalign): memcpy_fast=269ms memcpy=433 ms
result(dst unalign, src aligned): memcpy_fast=306ms memcpy=497 ms
result(dst unalign, src unalign): memcpy_fast=285ms memcpy=417 ms

benchmark(size=8388608 bytes, times=256):
result(dst aligned, src aligned): memcpy_fast=287ms memcpy=421 ms
result(dst aligned, src unalign): memcpy_fast=288ms memcpy=430 ms
result(dst unalign, src aligned): memcpy_fast=285ms memcpy=510 ms
result(dst unalign, src unalign): memcpy_fast=291ms memcpy=440 ms

benchmark random access:
memcpy_fast=487ms memcpy=1000ms

```


About
=====

skywind

http://www.skywind.me
