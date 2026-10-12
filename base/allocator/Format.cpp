#include <allocator/Format.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <fcntl.h>
#include <unistd.h>
#include <sys/syscall.h>

/// jemalloc: `JEMALLOC_EXPORT void (*je_malloc_message)(void *, const char *s);` (`src/malloc_io.c`)
/// Weak, so that tests linking the reference `lib_jemalloc.a` (which defines the same symbol) still link.
extern "C" __attribute__((weak, visibility("default"))) void (*je_malloc_message)(void * callback_argument, const char * s) = nullptr;

namespace jemalloc
{

namespace
{

/// Simple versions of assertion macros that do not recurse in case of assertion failures (`src/malloc_io.c:24-55`).
#define FORMAT_ASSERT(e) \
    do \
    { \
        if constexpr (config::debug) \
        { \
            if (!(e)) \
            { \
                writeMessage("<jemalloc>: Failed assertion\n"); \
                std::abort(); \
            } \
        } \
    } while (false)

#define FORMAT_NOT_REACHED() \
    do \
    { \
        if constexpr (config::debug) \
        { \
            writeMessage("<jemalloc>: Unreachable code reached\n"); \
            std::abort(); \
        } \
        ALLOCATOR_UNREACHABLE(); \
    } while (false)

#define FORMAT_ASSERT_NOT_IMPLEMENTED(e) \
    do \
    { \
        if constexpr (config::debug) \
        { \
            if (!(e)) \
            { \
                writeMessage("<jemalloc>: Not implemented\n"); \
                std::abort(); \
            } \
        } \
    } while (false)

/// UNSIGNED_TO_STRING_BUF_SIZE = (1 << (LG_SIZEOF_INTMAX_T + 3)) + 1.
constexpr size_t UNSIGNED_TO_STRING_BUF_SIZE = (size_t(1) << (3 + 3)) + 1;
constexpr size_t SIGNED_TO_STRING_BUF_SIZE = 1 + UNSIGNED_TO_STRING_BUF_SIZE;
constexpr size_t OCTAL_TO_STRING_BUF_SIZE = 1 + UNSIGNED_TO_STRING_BUF_SIZE;
constexpr size_t HEX_TO_STRING_BUF_SIZE = 2 + UNSIGNED_TO_STRING_BUF_SIZE;

static_assert(sizeof(uintmax_t) == 8);

/// Writes the digits backwards into `s[0 .. UNSIGNED_TO_STRING_BUF_SIZE)`, returns a pointer to the first digit.
/// jemalloc: u2s
char * unsignedToString(uintmax_t x, unsigned base, bool uppercase, char * s, size_t * segment_length_ptr)
{
    unsigned i = UNSIGNED_TO_STRING_BUF_SIZE - 1;
    s[i] = '\0';
    switch (base)
    {
        case 10:
            do
            {
                --i;
                s[i] = "0123456789"[x % uint64_t(10)];
                x /= uint64_t(10);
            } while (x > 0);
            break;
        case 16: {
            const char * digits = uppercase ? "0123456789ABCDEF" : "0123456789abcdef";
            do
            {
                --i;
                s[i] = digits[x & 0xf];
                x >>= 4;
            } while (x > 0);
            break;
        }
        default: {
            const char * digits = uppercase ? "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" : "0123456789abcdefghijklmnopqrstuvwxyz";
            FORMAT_ASSERT(base >= 2 && base <= 36);
            do
            {
                --i;
                s[i] = digits[x % uint64_t(base)];
                x /= uint64_t(base);
            } while (x > 0);
        }
    }

    *segment_length_ptr = UNSIGNED_TO_STRING_BUF_SIZE - 1 - i;
    return &s[i];
}

/// jemalloc: d2s
char * signedToString(intmax_t x, char sign, char * s, size_t * segment_length_ptr)
{
    bool negative = x < 0;
    /// `x = -x` in jemalloc; `INTMAX_MIN` wraps to itself, which then converts to 2^63.
    uintmax_t magnitude = negative ? uintmax_t(0) - uintmax_t(x) : uintmax_t(x);
    s = unsignedToString(magnitude, 10, false, s, segment_length_ptr);
    if (negative)
        sign = '-';
    switch (sign)
    {
        case '-':
            if (!negative)
                break;
            [[fallthrough]];
        case ' ':
        case '+':
            --s;
            ++(*segment_length_ptr);
            *s = sign;
            break;
        default: FORMAT_NOT_REACHED();
    }
    return s;
}

/// jemalloc: o2s
char * octalToString(uintmax_t x, bool alternate_form, char * s, size_t * segment_length_ptr)
{
    s = unsignedToString(x, 8, false, s, segment_length_ptr);
    if (alternate_form && *s != '0')
    {
        --s;
        ++(*segment_length_ptr);
        *s = '0';
    }
    return s;
}

/// jemalloc: x2s
char * hexToString(uintmax_t x, bool alternate_form, bool uppercase, char * s, size_t * segment_length_ptr)
{
    s = unsignedToString(x, 16, uppercase, s, segment_length_ptr);
    if (alternate_form)
    {
        s -= 2;
        (*segment_length_ptr) += 2;
        s[0] = '0';
        s[1] = uppercase ? 'X' : 'x';
    }
    return s;
}

/// The length codes of `GET_ARG_NUMERIC`: `'?'` (none), `'l'`, `'q'` (`ll`), `'j'`, `'t'`, `'z'`, `'p'` (synthetic);
/// unsigned conversions add 0x80.
constexpr unsigned char UNSIGNED_FLAG = 0x80;

/// jemalloc: malloc_write_fd_syscall
ssize_t writeFDSyscall(int fd, const void * buf, size_t count)
{
#if !defined(__APPLE__) && defined(SYS_write)
    if constexpr (config::use_syscall)
        /// Use syscall(2) rather than write(2) in order to avoid the possibility of memory allocation within libc.
        return static_cast<ssize_t>(::syscall(SYS_write, fd, buf, count));
    else
#endif
        return ::write(fd, buf, count);
}

/// jemalloc: malloc_read_fd_syscall
ssize_t readFDSyscall(int fd, void * buf, size_t count)
{
#if !defined(__APPLE__) && defined(SYS_read)
    if constexpr (config::use_syscall)
        return static_cast<ssize_t>(::syscall(SYS_read, fd, buf, count));
    else
#endif
        return ::read(fd, buf, count);
}

/// The two flavors of `strerror_r`: GNU (returns `char *`) and XSI (returns `int`).
[[maybe_unused]] int strerrorResult(int result, char *, size_t)
{
    return result;
}

[[maybe_unused]] int strerrorResult(char * b, char * buf, size_t buf_length)
{
    if (b != buf)
    {
        std::strncpy(buf, b, buf_length);
        buf[buf_length - 1] = '\0';
    }
    return 0;
}

static_assert(
    std::is_same_v<decltype(::strerror_r(0, static_cast<char *>(nullptr), size_t(0))), char *> == config::strerror_r_returns_char,
    "config::strerror_r_returns_char does not match the libc in use");

}

/// jemalloc: wrtmessage
void defaultWriteMessage(void *, const char * s)
{
    writeFD(STDERR_FILENO, s, std::strlen(s));
}

/// jemalloc: malloc_write
void writeMessage(const char * s)
{
    if (je_malloc_message != nullptr)
        je_malloc_message(nullptr, s);
    else
        defaultWriteMessage(nullptr, s);
}

/// jemalloc: buferror
int bufferError(int error, char * buf, size_t buf_length)
{
    return strerrorResult(::strerror_r(error, buf, buf_length), buf, buf_length);
}

/// jemalloc: malloc_strtoumax
uintmax_t strToUIntMax(const char * str, const char ** end_ptr, int base)
{
    uintmax_t result;
    uintmax_t digit;
    unsigned b;
    bool negative;
    const char * p = str;
    const char * ns;

    if (base < 0 || base == 1 || base > 36)
    {
        ns = p;
        errno = EINVAL;
        result = UINTMAX_MAX;
        goto label_return;
    }
    b = static_cast<unsigned>(base);

    /// Swallow leading whitespace and get sign, if any.
    negative = false;
    while (true)
    {
        switch (*p)
        {
            case '\t':
            case '\n':
            case '\v':
            case '\f':
            case '\r':
            case ' ': ++p; break;
            case '-': negative = true; [[fallthrough]];
            case '+': ++p; [[fallthrough]];
            default: goto label_prefix;
        }
    }

label_prefix:
    /// Note where the first non-whitespace/sign character is so that it is possible to tell whether any digits are
    /// consumed (e.g., "  0" vs. "  -x").
    ns = p;
    if (*p == '0')
    {
        switch (p[1])
        {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
                if (b == 0)
                    b = 8;
                if (b == 8)
                    ++p;
                break;
            case 'X':
            case 'x':
                switch (p[2])
                {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'A':
                    case 'B':
                    case 'C':
                    case 'D':
                    case 'E':
                    case 'F':
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                        if (b == 0)
                            b = 16;
                        if (b == 16)
                            p += 2;
                        break;
                    default: break;
                }
                break;
            default:
                /// jemalloc compatibility: "0" followed by anything else (including '8', '9' in base 10) yields 0
                /// with only the '0' consumed.
                ++p;
                result = 0;
                goto label_return;
        }
    }
    if (b == 0)
        b = 10;

    /// Convert.
    result = 0;
    while ((*p >= '0' && *p <= '9' && (digit = uintmax_t(*p - '0')) < b)
           || (*p >= 'A' && *p <= 'Z' && (digit = uintmax_t(10 + *p - 'A')) < b)
           || (*p >= 'a' && *p <= 'z' && (digit = uintmax_t(10 + *p - 'a')) < b))
    {
        uintmax_t previous_result = result;
        result *= b;
        result += digit;
        /// jemalloc compatibility: this misses some wraps.
        if (result < previous_result)
        {
            /// Overflow.
            errno = ERANGE;
            result = UINTMAX_MAX;
            goto label_return;
        }
        ++p;
    }
    if (negative)
        result = uintmax_t(0) - result; /// `(uintmax_t)(-((intmax_t)result))`

    if (p == ns)
    {
        /// No conversion performed.
        errno = EINVAL;
        result = UINTMAX_MAX;
        goto label_return;
    }

label_return:
    if (end_ptr != nullptr)
    {
        if (p == ns)
            *end_ptr = str; /// No characters were converted.
        else
            *end_ptr = p;
    }
    return result;
}

/// jemalloc: malloc_vsnprintf
ALLOCATOR_COLD size_t formatV(char * str, size_t size, const char * format_string, va_list args)
{
    size_t i = 0;
    const char * f = format_string;

    auto append_c = [&](char c)
    {
        if (i < size)
            str[i] = c;
        ++i;
    };

    auto append_s = [&](const char * s, size_t segment_length)
    {
        if (i < size)
        {
            size_t copy_length = (segment_length <= size - i) ? segment_length : size - i;
            std::memcpy(&str[i], s, copy_length);
        }
        i += segment_length;
    };

    auto append_padded_s = [&](const char * s, size_t segment_length, int width, bool left_justify, bool pad_zero)
    {
        /// Left padding.
        size_t pad_len = (width == -1) ? 0 : ((segment_length < size_t(width)) ? size_t(width) - segment_length : 0);
        if (!left_justify && pad_len != 0)
        {
            for (size_t j = 0; j < pad_len; ++j)
                append_c(pad_zero ? '0' : ' ');
        }
        /// Value.
        append_s(s, segment_length);
        /// Right padding.
        if (left_justify && pad_len != 0)
        {
            for (size_t j = 0; j < pad_len; ++j)
                append_c(' ');
        }
    };

    /// GET_ARG_NUMERIC for signed conversions.
    auto get_signed = [&](unsigned char len) -> intmax_t
    {
        switch (len)
        {
            case '?': return va_arg(args, int);
            case 'l': return va_arg(args, long);
            case 'q': return va_arg(args, long long);
            case 'j': return va_arg(args, intmax_t);
            case 't': return va_arg(args, ptrdiff_t);
            case 'z': return va_arg(args, ssize_t);
            default: FORMAT_NOT_REACHED();
        }
    };

    /// GET_ARG_NUMERIC for unsigned conversions (`len | 0x80`) and the synthetic `'p'`.
    auto get_unsigned = [&](unsigned char len) -> uintmax_t
    {
        switch (len)
        {
            case '?' | UNSIGNED_FLAG: return va_arg(args, unsigned int);
            case 'l' | UNSIGNED_FLAG: return va_arg(args, unsigned long);
            case 'q' | UNSIGNED_FLAG: return va_arg(args, unsigned long long);
            case 'j' | UNSIGNED_FLAG: return va_arg(args, uintmax_t);
            case 'z' | UNSIGNED_FLAG: return va_arg(args, size_t);
            case 'p': return reinterpret_cast<uintptr_t>(va_arg(args, void *));
            default:
                /// Includes `'t' | 0x80`: unsigned ptrdiff_t is not supported.
                FORMAT_NOT_REACHED();
        }
    };

    while (true)
    {
        switch (*f)
        {
            case '\0': goto label_out;
            case '%': {
                bool alternate_form = false;
                bool left_justify = false;
                bool plus_space = false;
                bool plus_plus = false;
                int precision = -1;
                int width = -1;
                unsigned char len = '?';
                const char * s;
                size_t segment_length;
                bool pad_zero = false;

                ++f;
                /// Flags. Note that '0' is not a flag: it is handled as the first digit of the width.
                while (true)
                {
                    switch (*f)
                    {
                        case '#':
                            FORMAT_ASSERT(!alternate_form);
                            alternate_form = true;
                            break;
                        case '-':
                            FORMAT_ASSERT(!left_justify);
                            left_justify = true;
                            break;
                        case ' ':
                            FORMAT_ASSERT(!plus_space);
                            plus_space = true;
                            break;
                        case '+':
                            FORMAT_ASSERT(!plus_plus);
                            plus_plus = true;
                            break;
                        default: goto label_width;
                    }
                    ++f;
                }
            label_width:
                /// Width.
                switch (*f)
                {
                    case '*':
                        width = va_arg(args, int);
                        ++f;
                        if (width < 0)
                        {
                            left_justify = true;
                            width = static_cast<int>(0u - static_cast<unsigned>(width));
                        }
                        break;
                    case '0': pad_zero = true; [[fallthrough]];
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9': {
                        /// jemalloc compatibility: parsed with `malloc_strtoumax`, so "%08u" yields width 0 and '8' as
                        /// the conversion specifier.
                        errno = 0;
                        uintmax_t unsigned_width = strToUIntMax(f, &f, 10);
                        FORMAT_ASSERT(unsigned_width != UINTMAX_MAX || errno != ERANGE);
                        width = static_cast<int>(unsigned_width);
                        break;
                    }
                    default: break;
                }
                /// Width/precision separator.
                if (*f == '.')
                    ++f;
                else
                    goto label_length;
                /// Precision.
                switch (*f)
                {
                    case '*':
                        precision = va_arg(args, int);
                        ++f;
                        break;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9': {
                        errno = 0;
                        uintmax_t unsigned_precision = strToUIntMax(f, &f, 10);
                        FORMAT_ASSERT(unsigned_precision != UINTMAX_MAX || errno != ERANGE);
                        precision = static_cast<int>(unsigned_precision);
                        break;
                    }
                    default: break;
                }
            label_length:
                /// Length.
                switch (*f)
                {
                    case 'l':
                        ++f;
                        if (*f == 'l')
                        {
                            len = 'q';
                            ++f;
                        }
                        else
                        {
                            len = 'l';
                        }
                        break;
                    case 'q':
                    case 'j':
                    case 't':
                    case 'z':
                        len = static_cast<unsigned char>(*f);
                        ++f;
                        break;
                    default: break;
                }
                /// Conversion specifier.
                switch (*f)
                {
                    case '%':
                        /// %%
                        append_c(*f);
                        ++f;
                        break;
                    case 'd':
                    case 'i': {
                        char buf[SIGNED_TO_STRING_BUF_SIZE];
                        /// Zero-padded negative numbers are not supported.
                        FORMAT_ASSERT(!pad_zero);
                        intmax_t value = get_signed(len);
                        s = signedToString(value, plus_plus ? '+' : (plus_space ? ' ' : '-'), buf, &segment_length);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    case 'o': {
                        char buf[OCTAL_TO_STRING_BUF_SIZE];
                        uintmax_t value = get_unsigned(len | UNSIGNED_FLAG);
                        s = octalToString(value, alternate_form, buf, &segment_length);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    case 'u': {
                        char buf[UNSIGNED_TO_STRING_BUF_SIZE];
                        uintmax_t value = get_unsigned(len | UNSIGNED_FLAG);
                        s = unsignedToString(value, 10, false, buf, &segment_length);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    case 'x':
                    case 'X': {
                        char buf[HEX_TO_STRING_BUF_SIZE];
                        uintmax_t value = get_unsigned(len | UNSIGNED_FLAG);
                        s = hexToString(value, alternate_form, *f == 'X', buf, &segment_length);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    case 'c': {
                        FORMAT_ASSERT(len == '?' || len == 'l');
                        FORMAT_ASSERT_NOT_IMPLEMENTED(len != 'l');
                        auto value = static_cast<unsigned char>(va_arg(args, int));
                        char buf[2];
                        buf[0] = static_cast<char>(value);
                        buf[1] = '\0';
                        append_padded_s(buf, 1, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    case 's':
                        FORMAT_ASSERT(len == '?' || len == 'l');
                        FORMAT_ASSERT_NOT_IMPLEMENTED(len != 'l');
                        s = va_arg(args, const char *);
                        /// jemalloc compatibility: with a precision, exactly `precision` bytes are copied.
                        segment_length = (precision < 0) ? std::strlen(s) : size_t(precision);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    case 'p': {
                        char buf[HEX_TO_STRING_BUF_SIZE];
                        uintmax_t value = get_unsigned('p');
                        s = hexToString(value, true, false, buf, &segment_length);
                        append_padded_s(s, segment_length, width, left_justify, pad_zero);
                        ++f;
                        break;
                    }
                    default: FORMAT_NOT_REACHED();
                }
                break;
            }
            default:
                append_c(*f);
                ++f;
                break;
        }
    }
label_out:
    if (i < size)
        str[i] = '\0';
    else
        str[size - 1] = '\0';

    return i;
}

/// jemalloc: malloc_snprintf
size_t format(char * str, size_t size, const char * format_string, ...)
{
    va_list args;
    va_start(args, format_string);
    size_t result = formatV(str, size, format_string, args);
    va_end(args);
    return result;
}

/// jemalloc: malloc_vcprintf
void printToCallbackV(WriteCallback * write_callback, void * callback_argument, const char * format_string, va_list args)
{
    char buf[MALLOC_PRINTF_BUF_SIZE];

    if (write_callback == nullptr)
    {
        /// The caller did not provide an alternate write_callback callback function, so use the default one.
        write_callback = messageCallback();
    }

    formatV(buf, sizeof(buf), format_string, args);
    write_callback(callback_argument, buf);
}

/// jemalloc: malloc_cprintf
void printToCallback(WriteCallback * write_callback, void * callback_argument, const char * format_string, ...)
{
    va_list args;
    va_start(args, format_string);
    printToCallbackV(write_callback, callback_argument, format_string, args);
    va_end(args);
}

/// jemalloc: malloc_printf
void printMessage(const char * format_string, ...)
{
    va_list args;
    va_start(args, format_string);
    printToCallbackV(nullptr, nullptr, format_string, args);
    va_end(args);
}

/// jemalloc: malloc_write_fd
ssize_t writeFD(int fd, const void * buf, size_t count)
{
    size_t bytes_written = 0;
    do
    {
        ssize_t result = writeFDSyscall(fd, static_cast<const char *>(buf) + bytes_written, count - bytes_written);
        if (result < 0)
        {
            if (errno == EINTR)
                continue;
            return result;
        }
        /// jemalloc compatibility: a zero-byte result loops forever.
        bytes_written += size_t(result);
    } while (bytes_written < count);
    return static_cast<ssize_t>(bytes_written);
}

/// jemalloc: malloc_read_fd
ssize_t readFD(int fd, void * buf, size_t count)
{
    size_t bytes_read = 0;
    do
    {
        ssize_t result = readFDSyscall(fd, static_cast<char *>(buf) + bytes_read, count - bytes_read);
        if (result < 0)
        {
            if (errno == EINTR)
                continue;
            return result;
        }
        if (result == 0)
            break;
        bytes_read += size_t(result);
    } while (bytes_read < count);
    return static_cast<ssize_t>(bytes_read);
}

/// jemalloc: malloc_open
int openFile(const char * path, int flags)
{
#if !defined(__APPLE__) && defined(SYS_open)
    if constexpr (config::use_syscall)
        return static_cast<int>(::syscall(SYS_open, path, flags));
    else
#elif !defined(__APPLE__) && defined(SYS_openat)
    if constexpr (config::use_syscall)
        return static_cast<int>(::syscall(SYS_openat, AT_FDCWD, path, flags));
    else
#endif
        return ::open(path, flags);
}

/// jemalloc: malloc_close
int closeFile(int fd)
{
#if !defined(__APPLE__) && defined(SYS_close)
    if constexpr (config::use_syscall)
        return static_cast<int>(::syscall(SYS_close, fd));
    else
#endif
        return ::close(fd);
}

/// jemalloc: malloc_lseek
off_t seekFile(int fd, off_t offset, int whence)
{
#if !defined(__APPLE__) && defined(SYS_lseek)
    if constexpr (config::use_syscall)
        return static_cast<off_t>(::syscall(SYS_lseek, fd, offset, whence));
    else
#endif
        return ::lseek(fd, offset, whence);
}

}
