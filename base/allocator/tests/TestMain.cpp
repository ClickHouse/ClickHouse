#include "Test.h"

#include <cstring>

/// Runs all tests (or those whose "suite.name" contains argv[1]).
int main(int argc, char ** argv)
{
    const char * filter = argc > 1 ? argv[1] : nullptr;
    int ran = 0;
    for (auto * test = allocator_test::testList(); test; test = test->next)
    {
        char full_name[512];
        std::snprintf(full_name, sizeof(full_name), "%s.%s", test->suite, test->name);
        if (filter && !std::strstr(full_name, filter))
            continue;

        int failures_before = allocator_test::failureCount();
        std::fprintf(stderr, "[ RUN  ] %s\n", full_name);
        test->function();
        bool ok = allocator_test::failureCount() == failures_before;
        std::fprintf(stderr, "[ %s ] %s\n", ok ? " OK " : "FAIL", full_name);
        ++ran;
    }

    std::fprintf(stderr, "%d tests, %d failed checks\n", ran, allocator_test::failureCount());
    return allocator_test::failureCount() == 0 ? 0 : 1;
}
