#include <allocator/Spin.h>

#include "Test.h"

using namespace jemalloc;

TEST(Spin, Adaptive)
{
    constinit static Spin global_spin{};
    CHECK_EQ(global_spin.iteration, 0u);

    Spin spin;
    for (unsigned i = 0; i < 5; ++i)
    {
        CHECK_EQ(spin.iteration, i);
        spin.adaptive();
    }
    /// After 5 rounds of spinning, it yields and no longer advances.
    CHECK_EQ(spin.iteration, 5u);
    spin.adaptive();
    CHECK_EQ(spin.iteration, 5u);
    spinCPUSpinwait();
}
