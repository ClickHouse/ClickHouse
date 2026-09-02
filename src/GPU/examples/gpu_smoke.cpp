/// Checks that an nvcc-compiled kernel and clang-compiled ClickHouse code link into one
/// working binary and agree across the C ABI boundary. Not a unit test -- it exists to
/// catch toolchain breakage, which is the failure mode this two-compiler setup invites.

#include <GPU/GPUDevice.h>

#include <cstdint>
#include <cstdio>
#include <numeric>
#include <vector>

namespace
{

int failures = 0;

void check(bool ok, const char * what)
{
    std::printf("%-40s %s\n", what, ok ? "PASS" : "FAIL");
    if (!ok)
        ++failures;
}

}

int main()
{
    using namespace DB::GPU;

    const int devices = deviceCount();
    std::printf("CUDA devices: %d\n", devices);

    if (devices == 0)
    {
        /// A host without a GPU is a supported configuration, so this is not a failure:
        /// it just means the kernels could not be exercised here.
        std::printf("no device available: %s\n", describe(Status::NoDevice).c_str());
        return 0;
    }

    std::printf("device 0:     %s\n\n", deviceName(0).c_str());

    constexpr size_t n = 1u << 20;

    std::vector<int64_t> a(n);
    std::vector<int64_t> b(n, 41);
    std::vector<int64_t> out(n, 0);
    std::iota(a.begin(), a.end(), int64_t(1));

    const Status add_status = addInt64(a, b, out);
    check(add_status == Status::Ok, "addInt64 status");
    if (add_status != Status::Ok)
        std::printf("  %s\n", describe(add_status).c_str());
    else
        check(out.front() == 42 && out.back() == int64_t(n) + 41, "addInt64 result");

    int64_t sum = 0;
    const Status sum_status = sumInt64(a, sum);
    check(sum_status == Status::Ok, "sumInt64 status");
    if (sum_status != Status::Ok)
        std::printf("  %s\n", describe(sum_status).c_str());
    else
        check(sum == std::accumulate(a.begin(), a.end(), int64_t(0)), "sumInt64 result");

    /// Empty input must be answered without touching the device -- an empty span's
    /// data() is null, which the kernels have to treat as valid.
    int64_t empty_sum = -1;
    check(sumInt64({}, empty_sum) == Status::Ok && empty_sum == 0, "sumInt64 of empty range");

    std::printf("\n%s\n", failures == 0 ? "all checks passed" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
