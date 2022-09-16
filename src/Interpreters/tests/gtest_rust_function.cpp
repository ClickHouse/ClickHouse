#include <gtest/gtest.h>
#include <rust_function.h>

TEST(RustFunction, Call)
{
    rust_function("abc");
}
