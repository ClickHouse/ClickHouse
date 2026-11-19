#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

#include <Common/Wyrd/Testing/Test.h>

#include <Common/ThreadPool.h>

#include <base/sanitizer_options.h>

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    GlobalThreadPool::shutdown();
    return result;
}

#endif
