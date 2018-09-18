Imported from https://github.com/llvm-project/llvm-project-20170507
revision: ad82e63b9719923cb393bd805730eaca0e3632a8

This is needed to avoid linking with "__cxa_thread_atexit_impl" function, that require too new (2.18) glibc library.

Note: "__cxa_thread_atexit_impl" may provide sophisticated implementation to correct destruction of thread-local objects,
that was created in different DSO. Read https://sourceware.org/glibc/wiki/Destructor%20support%20for%20thread_local%20variables
We simply don't need this implementation, because we don't use thread-local objects from different DSO.
