#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <iostream>

// - jemalloc:
//   real    0m10.816s
//   user    2m24.375s
//   sys     0m0.230s
// - tcmalloc:
//   PerCpuCachesActive: 1
//
//   GetProfileSamplingRate: -1
//   GetGuardedSamplingRate: -1
//
//   GetMaxPerCpuCacheSize: 3145728
//   GetMaxTotalThreadCacheBytes: 33554432
//
//   real    0m19.837s
//   user    4m32.754s
//   sys     0m3.329s

#ifdef USE_TCMALLOC_CPP
#include <tcmalloc/malloc_extension.h>
#include <tcmalloc/tcmalloc.h>
#include <tcmalloc/common.h>
void bootstrap()
{
    using ext = tcmalloc::MallocExtension;

    // XXX: does not changes anything...
    // ext::SetMaxTotalThreadCacheBytes(1 << 30);
    // ext::SetMaxPerCpuCacheSize(1 << 30);

    // NOTE: makes tcmalloc-cpp a little bit faster:
    //
    // - w/ sampling:
    //   GetProfileSamplingRate: 2097152
    //   GetGuardedSamplingRate: 104857600
    //
    //   real    0m22.121s
    //   user    5m20.928s
    //   sys     0m9.570s
    //
    // - w/o sampling:
    //   GetProfileSamplingRate: -1
    //   GetGuardedSamplingRate: -1
    //
    //   real    0m19.837s
    //   user    4m32.754s
    //   sys     0m3.329s
    ext::SetProfileSamplingRate(SIZE_MAX);
    ext::SetGuardedSamplingRate(SIZE_MAX);

    /// Also tried SCHEDULE_COOPERATIVE_AND_KERNEL -- no difference

    std::cerr << "tcmalloc:\n";
    std::cerr << '\n';
    std::cerr << "PerCpuCachesActive: " << ext::PerCpuCachesActive() << '\n';
    std::cerr << '\n';
    std::cerr << "GetProfileSamplingRate: " << ext::GetProfileSamplingRate() << '\n';
    std::cerr << "GetGuardedSamplingRate: " << ext::GetGuardedSamplingRate() << '\n';
    std::cerr << '\n';
    std::cerr << "GetMaxPerCpuCacheSize: " << ext::GetMaxPerCpuCacheSize() << '\n';
    std::cerr << "GetMaxTotalThreadCacheBytes: " << ext::GetMaxTotalThreadCacheBytes() << '\n';
    std::cerr << '\n';
    std::cerr << "kNumClasses: " << kNumClasses << '\n';
}
// XXX: does not helps anyway, but let's keep for now
void nfree(void *ptr, size_t size) { TCMallocInternalDeleteSized(ptr, size); }
#else
void bootstrap() {}
void nfree(void *ptr, size_t /*size*/) { free(ptr); }
#endif


void alloc_loop()
{
    for (size_t i = 0; i < 100; ++i)
    {
        size_t size = 4096;

        void * buf = malloc(size);
        if (!buf)
            abort();
        memset(buf, 0, size);

        /// tcmalloc is faster only when it uses front-end only [1], since only
        /// it allows parallel access w/o locks, otherwise locking is required
        /// and it is slower.
        ///
        ///   [1]: https://github.com/google/tcmalloc/blob/master/docs/design.md#the-tcmalloc-front-end
        ///
        /// If allocation is done only up to 256K (i.e. `size < 256<<10`):
        ///
        ///    - tcmalloc
        ///      real    0m2.335s
        ///      user    0m28.804s
        ///      sys     0m0.010s
        ///
        ///    - jemalloc
        ///      real    0m2.567s
        ///      user    0m32.748s
        ///      sys     0m0.020s
        while (size < 1048576)
        {
            size_t next_size = size * 4;

            nfree(buf, size);
            void * new_buf = malloc(next_size);
            if (!new_buf)
                abort();
            buf = new_buf;

            memset(reinterpret_cast<char*>(buf) + size, 0, next_size - size);
            size = next_size;
        }

        nfree(buf, size);
    }
}

void thread_func()
{
    for (size_t i = 0; i < 1000; ++i)
    {
        alloc_loop();
    }
}


int main(int, char **)
{
    bootstrap();

    std::vector<std::thread> threads(16);
    for (auto & thread : threads)
        thread = std::thread(thread_func);
    for (auto & thread : threads)
        thread.join();
    return 0;
}
