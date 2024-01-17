#include "StackTrace.h"

#include <base/FnTraits.h>
#include <base/constexpr_helpers.h>
#include <base/demangle.h>

#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/MemorySanitizer.h>
#include <Common/SymbolIndex.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <fmt/format.h>
#include <libunwind.h>

#include "config.h"

namespace
{
/// Currently this variable is set up once on server startup.
/// But we use atomic just in case, so it is possible to be modified at runtime.
std::atomic<bool> show_addresses = true;

bool shouldShowAddress(const void * addr)
{
    /// Likely inline frame
    if (!addr)
        return false;

    /// If the address is less than 4096, most likely it is a nullptr dereference with offset,
    /// and showing this offset is secure nevertheless.
    /// NOTE: 4096 is the page size on x86 and it can be different on other systems,
    /// but for the purpose of this branch, it does not matter.
    if (reinterpret_cast<uintptr_t>(addr) < 4096)
        return true;

    return show_addresses.load(std::memory_order_relaxed);
}
}

void StackTrace::setShowAddresses(bool show)
{
    show_addresses.store(show, std::memory_order_relaxed);
}

std::string SigsegvErrorString(const siginfo_t & info, [[maybe_unused]] const ucontext_t & context)
{
    using namespace std::string_literals;
    std::string address
        = info.si_addr == nullptr ? "NULL pointer"s : (shouldShowAddress(info.si_addr) ? fmt::format("{}", info.si_addr) : ""s);

    const std::string_view access =
#if defined(__x86_64__) && !defined(OS_FREEBSD) && !defined(OS_DARWIN) && !defined(__arm__) && !defined(__powerpc__)
        (context.uc_mcontext.gregs[REG_ERR] & 0x02) ? "write" : "read";
#else
        "";
#endif

    std::string_view message;

    switch (info.si_code)
    {
        case SEGV_ACCERR:
            message = "Attempted access has violated the permissions assigned to the memory area";
            break;
        case SEGV_MAPERR:
            message = "Address not mapped to object";
            break;
        default:
            message = "Unknown si_code";
            break;
    }

    return fmt::format("Address: {}. Access: {}. {}.", std::move(address), access, message);
}

constexpr std::string_view SigbusErrorString(int si_code)
{
    switch (si_code)
    {
        case BUS_ADRALN:
            return "Invalid address alignment.";
        case BUS_ADRERR:
            return "Non-existent physical address.";
        case BUS_OBJERR:
            return "Object specific hardware error.";

            // Linux specific
#if defined(BUS_MCEERR_AR)
        case BUS_MCEERR_AR:
            return "Hardware memory error: action required.";
#endif
#if defined(BUS_MCEERR_AO)
        case BUS_MCEERR_AO:
            return "Hardware memory error: action optional.";
#endif
        default:
            return "Unknown si_code.";
    }
}

constexpr std::string_view SigfpeErrorString(int si_code)
{
    switch (si_code)
    {
        case FPE_INTDIV:
            return "Integer divide by zero.";
        case FPE_INTOVF:
            return "Integer overflow.";
        case FPE_FLTDIV:
            return "Floating point divide by zero.";
        case FPE_FLTOVF:
            return "Floating point overflow.";
        case FPE_FLTUND:
            return "Floating point underflow.";
        case FPE_FLTRES:
            return "Floating point inexact result.";
        case FPE_FLTINV:
            return "Floating point invalid operation.";
        case FPE_FLTSUB:
            return "Subscript out of range.";
        default:
            return "Unknown si_code.";
    }
}

constexpr std::string_view SigillErrorString(int si_code)
{
    switch (si_code)
    {
        case ILL_ILLOPC:
            return "Illegal opcode.";
        case ILL_ILLOPN:
            return "Illegal operand.";
        case ILL_ILLADR:
            return "Illegal addressing mode.";
        case ILL_ILLTRP:
            return "Illegal trap.";
        case ILL_PRVOPC:
            return "Privileged opcode.";
        case ILL_PRVREG:
            return "Privileged register.";
        case ILL_COPROC:
            return "Coprocessor error.";
        case ILL_BADSTK:
            return "Internal stack error.";
        default:
            return "Unknown si_code.";
    }
}

std::string signalToErrorMessage(int sig, const siginfo_t & info, [[maybe_unused]] const ucontext_t & context)
{
    switch (sig)
    {
        case SIGSEGV:
            return SigsegvErrorString(info, context);
        case SIGBUS:
            return std::string{SigbusErrorString(info.si_code)};
        case SIGILL:
            return std::string{SigillErrorString(info.si_code)};
        case SIGFPE:
            return std::string{SigfpeErrorString(info.si_code)};
        case SIGTSTP:
            return "This is a signal used for debugging purposes by the user.";
        default:
            return "";
    }
}

static void * getCallerAddress(const ucontext_t & context)
{
#if defined(__x86_64__)
    /// Get the address at the time the signal was raised from the RIP (x86-64)
#    if defined(OS_FREEBSD)
    return reinterpret_cast<void *>(context.uc_mcontext.mc_rip);
#    elif defined(OS_DARWIN)
    return reinterpret_cast<void *>(context.uc_mcontext->__ss.__rip);
#    else
    return reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);
#    endif
#elif defined(OS_DARWIN) && defined(__aarch64__)
    return reinterpret_cast<void *>(context.uc_mcontext->__ss.__pc);
#elif defined(OS_FREEBSD) && defined(__aarch64__)
    return reinterpret_cast<void *>(context.uc_mcontext.mc_gpregs.gp_elr);
#elif defined(__aarch64__)
    return reinterpret_cast<void *>(context.uc_mcontext.pc);
#elif defined(__powerpc64__) && defined(__linux__)
    return reinterpret_cast<void *>(context.uc_mcontext.gp_regs[PT_NIP]);
#elif defined(__powerpc64__) && defined(__FreeBSD__)
    return reinterpret_cast<void *>(context.uc_mcontext.mc_srr0);
#elif defined(__riscv)
    return reinterpret_cast<void *>(context.uc_mcontext.__gregs[REG_PC]);
#elif defined(__s390x__)
    return reinterpret_cast<void *>(context.uc_mcontext.psw.addr);
#else
    return nullptr;
#endif
}

void StackTrace::forEachFrame(
    const StackTrace::FramePointers & frame_pointers,
    size_t offset,
    size_t size,
    std::function<void(const Frame &)> callback,
    bool fatal)
{
#if defined(__ELF__) && !defined(OS_FREEBSD)
    const DB::SymbolIndex & symbol_index = DB::SymbolIndex::instance();
    std::unordered_map<std::string, DB::Dwarf> dwarfs;

    using enum DB::Dwarf::LocationInfoMode;
    const auto mode = fatal ? FULL_WITH_INLINE : FAST;

    for (size_t i = offset; i < size; ++i)
    {
        StackTrace::Frame current_frame;
        std::vector<DB::Dwarf::SymbolizedFrame> inline_frames;
        current_frame.virtual_addr = frame_pointers[i];
        const auto * object = symbol_index.findObject(current_frame.virtual_addr);
        uintptr_t virtual_offset = object ? uintptr_t(object->address_begin) : 0;
        current_frame.physical_addr = reinterpret_cast<void *>(uintptr_t(current_frame.virtual_addr) - virtual_offset);

        if (object)
        {
            current_frame.object = object->name;
            if (std::error_code ec; std::filesystem::exists(current_frame.object.value(), ec) && !ec)
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;

                DB::Dwarf::LocationInfo location;
                if (dwarf_it->second.findAddress(
                        uintptr_t(current_frame.physical_addr), location, mode, inline_frames))
                {
                    current_frame.file = location.file.toString();
                    current_frame.line = location.line;
                }
            }
        }

        if (const auto * symbol = symbol_index.findSymbol(current_frame.virtual_addr))
            current_frame.symbol = demangle(symbol->name);

        for (const auto & frame : inline_frames)
        {
            StackTrace::Frame current_inline_frame;
            const String file_for_inline_frame = frame.location.file.toString();

            current_inline_frame.file = "inlined from " + file_for_inline_frame;
            current_inline_frame.line = frame.location.line;
            current_inline_frame.symbol = frame.name;

            callback(current_inline_frame);
        }

        callback(current_frame);
    }
#else
    UNUSED(fatal);

    for (size_t i = offset; i < size; ++i)
    {
        StackTrace::Frame current_frame;
        current_frame.virtual_addr = frame_pointers[i];
        callback(current_frame);
    }
#endif
}

StackTrace::StackTrace(const ucontext_t & signal_context)
{
    tryCapture();

    /// This variable from signal handler is not instrumented by Memory Sanitizer.
    __msan_unpoison(&signal_context, sizeof(signal_context));

    void * caller_address = getCallerAddress(signal_context);

    if (size == 0 && caller_address)
    {
        frame_pointers[0] = caller_address;
        size = 1;
    }
    else
    {
        /// Skip excessive stack frames that we have created while finding stack trace.
        for (size_t i = 0; i < size; ++i)
        {
            if (frame_pointers[i] == caller_address)
            {
                offset = i;
                break;
            }
        }
    }
}

void StackTrace::tryCapture()
{
    size = unw_backtrace(frame_pointers.data(), capacity);
    __msan_unpoison(frame_pointers.data(), size * sizeof(frame_pointers[0]));
}

/// ClickHouse uses bundled libc++ so type names will be the same on every system thus it's safe to hardcode them
constexpr std::pair<std::string_view, std::string_view> replacements[]
    = {{"::__1", ""}, {"std::basic_string<char, std::char_traits<char>, std::allocator<char>>", "String"}};

// Demangle @c symbol_name if it's not from __functional header (as such functions don't provide any useful
// information but pollute stack traces).
// Replace parts from @c replacements with shorter aliases
String demangleAndCollapseNames(std::string_view file, const char * const symbol_name)
{
    if (!symbol_name)
        return "?";

    std::string_view file_copy = file;
    if (auto trim_pos = file.find_last_of('/'); trim_pos != file.npos)
        file_copy.remove_suffix(file.size() - trim_pos);
    if (file_copy.ends_with("functional"))
        return "?";

    String haystack = demangle(symbol_name);

    // TODO myrrc surely there is a written version already for better in place search&replace
    for (auto [needle, to] : replacements)
    {
        size_t pos = 0;
        while ((pos = haystack.find(needle, pos)) != std::string::npos)
        {
            haystack.replace(pos, needle.length(), to);
            pos += to.length();
        }
    }

    return haystack;
}

struct StackTraceRefTriple
{
    const StackTrace::FramePointers & pointers;
    size_t offset;
    size_t size;
};

struct StackTraceTriple
{
    StackTrace::FramePointers pointers;
    size_t offset;
    size_t size;
};

template <class T>
concept MaybeRef = std::is_same_v<T, StackTraceTriple> || std::is_same_v<T, StackTraceRefTriple>;

constexpr bool operator<(const MaybeRef auto & left, const MaybeRef auto & right)
{
    return std::tuple{left.pointers, left.size, left.offset} < std::tuple{right.pointers, right.size, right.offset};
}

static void
toStringEveryLineImpl([[maybe_unused]] bool fatal, const StackTraceRefTriple & stack_trace, Fn<void(std::string_view)> auto && callback)
{
    if (stack_trace.size == 0)
        return callback("<Empty trace>");

    size_t frame_index = stack_trace.offset;
#if defined(__ELF__) && !defined(OS_FREEBSD)
    size_t inline_frame_index = 0;
    auto callback_wrapper = [&](const StackTrace::Frame & frame)
    {
        DB::WriteBufferFromOwnString out;

        /// Inline frame
        if (!frame.virtual_addr)
        {
            out << frame_index << "." << inline_frame_index++ << ". ";
        }
        else
        {
            out << frame_index++ << ". ";
            inline_frame_index = 0;
        }

        if (frame.file.has_value() && frame.line.has_value())
            out << *frame.file << ':' << *frame.line << ": ";

        if (frame.symbol.has_value() && frame.file.has_value())
            out << demangleAndCollapseNames(*frame.file, frame.symbol->data());
        else
            out << "?";

        if (shouldShowAddress(frame.physical_addr))
        {
            out << " @ ";
            DB::writePointerHex(frame.physical_addr, out);
        }

        if (frame.object.has_value())
            out << " in " << *frame.object;

        callback(out.str());
    };
#else
    auto callback_wrapper = [&](const StackTrace::Frame & frame)
    {
        if (frame.virtual_addr && shouldShowAddress(frame.virtual_addr))
            callback(fmt::format("{}. {}", frame_index++, frame.virtual_addr));
    };
#endif

    StackTrace::forEachFrame(stack_trace.pointers, stack_trace.offset, stack_trace.size, callback_wrapper, fatal);
}

void StackTrace::toStringEveryLine(std::function<void(std::string_view)> callback) const
{
    toStringEveryLineImpl(true, {frame_pointers, offset, size}, std::move(callback));
}

void StackTrace::toStringEveryLine(const FramePointers & frame_pointers, std::function<void(std::string_view)> callback)
{
    toStringEveryLineImpl(true, {frame_pointers, 0, static_cast<size_t>(std::ranges::find(frame_pointers, nullptr) - frame_pointers.begin())}, std::move(callback));
}

void StackTrace::toStringEveryLine(void ** frame_pointers_raw, size_t offset, size_t size, std::function<void(std::string_view)> callback)
{
    __msan_unpoison(frame_pointers_raw, size * sizeof(*frame_pointers_raw));

    StackTrace::FramePointers frame_pointers{};
    std::copy_n(frame_pointers_raw, size, frame_pointers.begin());

    toStringEveryLineImpl(true, {frame_pointers, offset, size}, std::move(callback));
}

using StackTraceCache = std::map<StackTraceTriple, String, std::less<>>;

static StackTraceCache & cacheInstance()
{
    static StackTraceCache cache;
    return cache;
}

static std::mutex stacktrace_cache_mutex;

String toStringCached(const StackTrace::FramePointers & pointers, size_t offset, size_t size)
{
    /// Calculation of stack trace text is extremely slow.
    /// We use simple cache because otherwise the server could be overloaded by trash queries.
    /// Note that this cache can grow unconditionally, but practically it should be small.
    std::lock_guard lock{stacktrace_cache_mutex};

    StackTraceCache & cache = cacheInstance();
    const StackTraceRefTriple key{pointers, offset, size};

    if (auto it = cache.find(key); it != cache.end())
        return it->second;
    else
    {
        DB::WriteBufferFromOwnString out;
        toStringEveryLineImpl(false, key, [&](std::string_view str) { out << str << '\n'; });

        return cache.emplace(StackTraceTriple{pointers, offset, size}, out.str()).first->second;
    }
}

std::string StackTrace::toString() const
{
    return toStringCached(frame_pointers, offset, size);
}

std::string StackTrace::toString(void ** frame_pointers_raw, size_t offset, size_t size)
{
    __msan_unpoison(frame_pointers_raw, size * sizeof(*frame_pointers_raw));

    StackTrace::FramePointers frame_pointers{};
    std::copy_n(frame_pointers_raw, size, frame_pointers.begin());

    return toStringCached(frame_pointers, offset, size);
}

void StackTrace::dropCache()
{
    std::lock_guard lock{stacktrace_cache_mutex};
    cacheInstance().clear();
}
