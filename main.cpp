/// clang++ -nostdinc -nostdinc++ -nostdlib -nodefaultlibs -static -g -O3 main.cpp && ./a.out 1 2 3
/// clang++ -nostdinc -nostdinc++ -nostdlib -nodefaultlibs -shared -fPIC -O3 -g main.cpp && ./a.out 1 2 3

#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))

#define __NR_write 1
#define __NR_exit_group 231

#define STDERR_FILENO 2


/// For auxv
#define AT_NULL         0
#define AT_IGNORE       1
#define AT_EXECFD       2
#define AT_PHDR         3
#define AT_PHENT        4
#define AT_PHNUM        5
#define AT_PAGESZ       6
#define AT_BASE         7
#define AT_FLAGS        8
#define AT_ENTRY        9
#define AT_NOTELF       10
#define AT_UID          11
#define AT_EUID         12
#define AT_GID          13
#define AT_EGID         14
#define AT_CLKTCK       17
#define AT_PLATFORM     15
#define AT_HWCAP        16
#define AT_FPUCW        18
#define AT_DCACHEBSIZE  19
#define AT_ICACHEBSIZE  20
#define AT_UCACHEBSIZE  21
#define AT_IGNOREPPC    22
#define AT_SECURE       23
#define AT_BASE_PLATFORM 24
#define AT_RANDOM       25
#define AT_HWCAP2       26
#define AT_EXECFN       31
#define AT_SYSINFO      32
#define AT_SYSINFO_EHDR 33
#define AT_L1I_CACHESHAPE       34
#define AT_L1D_CACHESHAPE       35
#define AT_L2_CACHESHAPE        36
#define AT_L3_CACHESHAPE        37
#define AT_L1I_CACHESIZE        40
#define AT_L1I_CACHEGEOMETRY    41
#define AT_L1D_CACHESIZE        42
#define AT_L1D_CACHEGEOMETRY    43
#define AT_L2_CACHESIZE         44
#define AT_L2_CACHEGEOMETRY     45
#define AT_L3_CACHESIZE         46
#define AT_L3_CACHEGEOMETRY     47
#define AT_MINSIGSTKSZ          51


using int64_t = long;
using ssize_t = long;
using intptr_t = long;
using size_t = unsigned long;


/// These symbols have hidden linkage to avoid them being called through PLT.
/// It allows to use it before dynamic linking being performed.
namespace
{

NO_INLINE long internal_syscall_asm(long x0 = 0, long x1 = 0, long x2 = 0, long x3 = 0, long x4 = 0, long x5 = 0, long x6 = 0)
{
    long ret;
    __asm__ __volatile__ (R"(
        movq %[x0],%%rax;
        movq %[x1],%%rdi;
        movq %[x2],%%rsi;
        movq %[x3],%%rdx;
        movq %[x4],%%r10;
        movq %[x5],%%r8;
        movq %[x6],%%r9;
        syscall;
        movq %%rax,%[ret]
    )"
    : [ret]"=g"(ret)
    : [x0]"g"(x0), [x1]"g"(x1), [x2]"g"(x2), [x3]"g"(x3), [x4]"g"(x4), [x5]"g"(x5), [x6]"g"(x6)
    : "memory");
    return ret;
}

template <typename... Ts>
ALWAYS_INLINE long internal_syscall(Ts... args)
{
    return internal_syscall_asm((long)(args)...);
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

void print_string(const char * message)
{
    write(STDERR_FILENO, message, strlen(message));
}

void print_number(size_t num)
{
    char buf[20];
    char * pos = &buf[20];

    do
    {
        --pos;
        *pos = '0' + (num % 10);
        num /= 10;
    } while (num);

    write(STDERR_FILENO, pos, &buf[20] - pos);
}

void ALWAYS_INLINE assert(bool x)
{
    if (!x)
        __builtin_trap();
}

}


extern "C"
{

int main(int argc, char ** argv, char ** envp)
{
    print_string("Hello, world!\n");
    return 0;
}


[[noreturn]] void _start()
{
    intptr_t * stack = static_cast<intptr_t *>(__builtin_frame_address(0));

    size_t argc = stack[1];
    char ** argv = reinterpret_cast<char **>(&stack[2]);
    assert(argv[argc] == nullptr);

    char ** envp = &argv[argc + 1];
    size_t envc = 0;
    for (size_t i = 0; envp[i]; ++i)
        ++envc;

    assert(envp[envc] == nullptr);

    char ** auxv = &envp[envc + 1];
    size_t auxc = 0;
    for (size_t i = 0; auxv[i]; ++i)
        ++auxc;

    print_number(argc);

    print_string("\n");

    for (size_t i = 0; i < argc; ++i)
    {
        print_string(argv[i]);
        print_string("\n");
    }

    print_string("\n");

    for (size_t i = 0; envp[i]; ++i)
    {
        print_string(envp[i]);
        print_string("\n");
    }

    print_string("\n");
    print_number(auxc);

    _exit(main(argc, argv, envp));
}

}
