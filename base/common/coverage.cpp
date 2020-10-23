#include <common/coverage.h>
#include <common/config_common.h>

#if WITH_COVERAGE

#include <unistd.h>
#include <mutex>

#if  defined(__clang__)
extern "C" void __llvm_profile_dump();
#elif defined(__GNUC__) || defined(__GNUG__)
extern "C" void __gcov_exit();
#endif

#endif


void dumpCoverageReportIfPossible()
{
#if WITH_COVERAGE
    static std::mutex mutex;
    std::lock_guard lock(mutex);

#if defined(__clang__)
    __llvm_profile_dump();
#elif defined(__GNUC__) || defined(__GNUG__)
    __gcov_exit();
#endif

#endif
}
