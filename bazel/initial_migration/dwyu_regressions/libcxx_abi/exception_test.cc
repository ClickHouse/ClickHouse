#include <cstddef>
#include <cstdio>

extern "C" std::size_t a_exception_size(int);
extern "C" std::size_t a_exception_align(int);
extern "C" std::size_t b_exception_size(int);
extern "C" std::size_t b_exception_align(int);
extern "C" void a_raise(int);
extern "C" void b_raise(int);
extern "C" int a_catch(int, void (*)(int));
extern "C" int b_catch(int, void (*)(int));

int main()
{
    for (int kind = 0; kind != 3; ++kind)
    {
        const std::size_t left_size = a_exception_size(kind);
        const std::size_t right_size = b_exception_size(kind);
        const std::size_t left_align = a_exception_align(kind);
        const std::size_t right_align = b_exception_align(kind);
        std::printf("LIBCXX_ABI_LAYOUT kind=%d left_size=%zu right_size=%zu left_align=%zu right_align=%zu\n",
                    kind, left_size, right_size, left_align, right_align);
        if (left_size != right_size || left_align != right_align)
        {
            std::puts("LIBCXX_ABI_LAYOUT_MISMATCH");
            return 42;
        }
    }
    for (int kind = 0; kind != 4; ++kind)
    {
        const int left_catch = a_catch(kind, b_raise);
        const int right_catch = b_catch(kind, a_raise);
        std::printf("LIBCXX_ABI_CROSS kind=%d left_catch=%d right_catch=%d\n", kind, left_catch, right_catch);
        if (left_catch != 0 || right_catch != 0)
            return 43;
    }
    std::puts("LIBCXX_ABI_MIXED_PASSED");
    return 0;
}
