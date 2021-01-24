/// clang++ -nostdinc -nostdinc++ -nostdlib -nodefaultlibs -static -O3 main.cpp

#define NO_INLINE __attribute__((__noinline__))

#define __NR_write 1
#define __NR_exit_group 231

#define STDERR_FILENO 2


using int64_t = long;
using ssize_t = long;
using size_t = unsigned long;


extern "C"
{

namespace
{

__attribute__((__noinline__)) int64_t internal_syscall(...)
{
    __asm__ __volatile__ (R"(
        movq %%rdi,%%rax;
        movq %%rsi,%%rdi;
        movq %%rdx,%%rsi;
        movq %%rcx,%%rdx;
        movq %%r8,%%r10;
        movq %%r9,%%r8;
        movq 8(%%rsp),%%r9;
        syscall;
        ret
    )" : : : "memory");
    return 0;
}


[[noreturn]] void _exit(int code)
{
    internal_syscall(__NR_exit_group, code);
    __builtin_unreachable();
}

ssize_t write(int fd, const void * buf, size_t count)
{
    return internal_syscall(__NR_write, static_cast<long>(fd), buf, count);
}

size_t strlen(const char * s)
{
    const char * end = s;
    while (*end)
        ++end;
    return end - s;
}

void print_error(const char * message)
{
    write(STDERR_FILENO, message, __builtin_strlen(message));
}

}


[[noreturn]] void _start(long * p)
{
    print_error("Hello, world!\n");
    _exit(0);
}

}
