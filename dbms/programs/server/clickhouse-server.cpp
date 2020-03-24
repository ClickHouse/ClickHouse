#include <new>

#include <common/phdr_cache.h>


int mainEntryClickHouseServer(int argc, char ** argv);

/**
  * This is the entry-point for the split build server. The initialization
  * is copied from single-binary entry point in main.cpp.
  */
int main(int argc_, char ** argv_)
{
    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    updatePHDRCache();

    return mainEntryClickHouseServer(argc_, argv_);
}
