Internal implementation of `memcpy` function.

It has the following advantages over `libc`-supplied implementation:
- it is linked statically, so the function is called directly, not through a `PLT` (procedure lookup table of shared library);
- it is linked statically, so the function can have position-dependent code;
- your binaries will not depend on `glibc`'s memcpy, that forces dependency on specific symbol version like `memcpy@@GLIBC_2.14` and consequently on specific version of `glibc` library;
- you can include `memcpy.h` directly and the function has the chance to be inlined, which is beneficial for small but unknown at compile time sizes of memory regions;
- this version of `memcpy` pretend to be faster (in our benchmarks, the difference is within few percents).

Currently it uses the implementation from **Linwei** (skywind3000@163.com).
Look at https://www.zhihu.com/question/35172305 for discussion.

Drawbacks:
- only use SSE 2, doesn't use wider (AVX, AVX 512) vector registers when available;
- no CPU dispatching; doesn't take into account actual cache size.

Also worth to look at:
- simple implementation from Facebook: https://github.com/facebook/folly/blob/master/folly/memcpy.S
- implementation from Agner Fog: http://www.agner.org/optimize/
- glibc source code.
