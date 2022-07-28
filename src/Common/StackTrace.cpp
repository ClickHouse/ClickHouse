#include <Common/StackTrace.h>

#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/SymbolIndex.h>
#include <Common/MemorySanitizer.h>
#include <base/CachedFn.h>
#include <base/demangle.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <unordered_map>

#include <Common/config.h>

#if USE_UNWIND
#    include <libunwind.h>
#endif


namespace
{
    /// Currently this variable is set up once on server startup.
    /// But we use atomic just in case, so it is possible to be modified at runtime.
    std::atomic<bool> show_addresses = true;
}

void StackTrace::setShowAddresses(bool show)
{
    show_addresses.store(show, std::memory_order_relaxed);
}


std::string signalToErrorMessage(int sig, const siginfo_t & info, [[maybe_unused]] const ucontext_t & context)
{
    std::stringstream error;        // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    error.exceptions(std::ios::failbit);
    switch (sig)
    {
        case SIGSEGV:
        {
            /// Print info about address and reason.
            if (nullptr == info.si_addr)
                error << "Address: NULL pointer.";
            else if (show_addresses.load(std::memory_order_relaxed))
                error << "Address: " << info.si_addr;

#if defined(__x86_64__) && !defined(OS_FREEBSD) && !defined(OS_DARWIN) && !defined(__arm__) && !defined(__powerpc__)
            auto err_mask = context.uc_mcontext.gregs[REG_ERR];
            if ((err_mask & 0x02))
                error << " Access: write.";
            else
                error << " Access: read.";
#endif

            switch (info.si_code)
            {
                case SEGV_ACCERR:
                    error << " Attempted access has violated the permissions assigned to the memory area.";
                    break;
                case SEGV_MAPERR:
                    error << " Address not mapped to object.";
                    break;
                default:
                    error << " Unknown si_code.";
                    break;
            }
            break;
        }

        case SIGBUS:
        {
            switch (info.si_code)
            {
                case BUS_ADRALN:
                    error << "Invalid address alignment.";
                    break;
                case BUS_ADRERR:
                    error << "Non-existent physical address.";
                    break;
                case BUS_OBJERR:
                    error << "Object specific hardware error.";
                    break;

                    // Linux specific
#if defined(BUS_MCEERR_AR)
                case BUS_MCEERR_AR:
                    error << "Hardware memory error: action required.";
                    break;
#endif
#if defined(BUS_MCEERR_AO)
                case BUS_MCEERR_AO:
                    error << "Hardware memory error: action optional.";
                    break;
#endif

                default:
                    error << "Unknown si_code.";
                    break;
            }
            break;
        }

        case SIGILL:
        {
            switch (info.si_code)
            {
                case ILL_ILLOPC:
                    error << "Illegal opcode.";
                    break;
                case ILL_ILLOPN:
                    error << "Illegal operand.";
                    break;
                case ILL_ILLADR:
                    error << "Illegal addressing mode.";
                    break;
                case ILL_ILLTRP:
                    error << "Illegal trap.";
                    break;
                case ILL_PRVOPC:
                    error << "Privileged opcode.";
                    break;
                case ILL_PRVREG:
                    error << "Privileged register.";
                    break;
                case ILL_COPROC:
                    error << "Coprocessor error.";
                    break;
                case ILL_BADSTK:
                    error << "Internal stack error.";
                    break;
                default:
                    error << "Unknown si_code.";
                    break;
            }
            break;
        }

        case SIGFPE:
        {
            switch (info.si_code)
            {
                case FPE_INTDIV:
                    error << "Integer divide by zero.";
                    break;
                case FPE_INTOVF:
                    error << "Integer overflow.";
                    break;
                case FPE_FLTDIV:
                    error << "Floating point divide by zero.";
                    break;
                case FPE_FLTOVF:
                    error << "Floating point overflow.";
                    break;
                case FPE_FLTUND:
                    error << "Floating point underflow.";
                    break;
                case FPE_FLTRES:
                    error << "Floating point inexact result.";
                    break;
                case FPE_FLTINV:
                    error << "Floating point invalid operation.";
                    break;
                case FPE_FLTSUB:
                    error << "Subscript out of range.";
                    break;
                default:
                    error << "Unknown si_code.";
                    break;
            }
            break;
        }

        case SIGTSTP:
        {
            error << "This is a signal used for debugging purposes by the user.";
            break;
        }
    }

    return error.str();
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
#elif defined(__powerpc64__)
    return reinterpret_cast<void *>(context.uc_mcontext.gp_regs[PT_NIP]);
#elif defined(__riscv)
    return reinterpret_cast<void *>(context.uc_mcontext.__gregs[REG_PC]);
#else
    return nullptr;
#endif
}

void StackTrace::symbolize(
    const StackTrace::FramePointers & frame_pointers, [[maybe_unused]] size_t offset,
    size_t size, StackTrace::Frames & frames)
{
#if defined(__ELF__) && !defined(OS_FREEBSD)

    auto symbol_index_ptr = DB::SymbolIndex::instance();
    const DB::SymbolIndex & symbol_index = *symbol_index_ptr;
    std::unordered_map<std::string, DB::Dwarf> dwarfs;

    for (size_t i = 0; i < offset; ++i)
    {
        frames[i].virtual_addr = frame_pointers[i];
    }

    for (size_t i = offset; i < size; ++i)
    {
        StackTrace::Frame & current_frame = frames[i];
        current_frame.virtual_addr = frame_pointers[i];
        const auto * object = symbol_index.findObject(current_frame.virtual_addr);
        uintptr_t virtual_offset = object ? uintptr_t(object->address_begin) : 0;
        current_frame.physical_addr = reinterpret_cast<void *>(uintptr_t(current_frame.virtual_addr) - virtual_offset);

        if (object)
        {
            current_frame.object = object->name;
            if (std::filesystem::exists(current_frame.object.value()))
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;

                DB::Dwarf::LocationInfo location;
                std::vector<DB::Dwarf::SymbolizedFrame> inline_frames;
                if (dwarf_it->second.findAddress(
                        uintptr_t(current_frame.physical_addr), location, DB::Dwarf::LocationInfoMode::FAST, inline_frames))
                {
                    current_frame.file = location.file.toString();
                    current_frame.line = location.line;
                }
            }
        }
        else
        {
            current_frame.object = "?";
        }

        const auto * symbol = symbol_index.findSymbol(current_frame.virtual_addr);
        if (symbol)
        {
            int status = 0;
            current_frame.symbol = demangle(symbol->name, status);
        }
        else
        {
            current_frame.symbol = "?";
        }
    }
#else
    for (size_t i = 0; i < size; ++i)
    {
        frames[i].virtual_addr = frame_pointers[i];
    }
#endif
}

StackTrace::StackTrace()
{
    tryCapture();
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

StackTrace::StackTrace(NoCapture)
{
}

void StackTrace::tryCapture()
{
    size = 0;
#if USE_UNWIND
    size = unw_backtrace(frame_pointers.data(), capacity);
    __msan_unpoison(frame_pointers.data(), size * sizeof(frame_pointers[0]));
#endif
}

size_t StackTrace::getSize() const
{
    return size;
}

size_t StackTrace::getOffset() const
{
    return offset;
}

const StackTrace::FramePointers & StackTrace::getFramePointers() const
{
    return frame_pointers;
}

static void toStringEveryLineImpl(
    [[maybe_unused]] bool fatal,
    const StackTrace::FramePointers & frame_pointers,
    size_t offset,
    size_t size,
    std::function<void(const std::string &)> callback)
{
    if (size == 0)
        return callback("<Empty trace>");

#if defined(__ELF__) && !defined(OS_FREEBSD)
    auto symbol_index_ptr = DB::SymbolIndex::instance();
    const DB::SymbolIndex & symbol_index = *symbol_index_ptr;
    std::unordered_map<std::string, DB::Dwarf> dwarfs;

    std::stringstream out;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out.exceptions(std::ios::failbit);

    for (size_t i = offset; i < size; ++i)
    {
        std::vector<DB::Dwarf::SymbolizedFrame> inline_frames;
        const void * virtual_addr = frame_pointers[i];
        const auto * object = symbol_index.findObject(virtual_addr);
        uintptr_t virtual_offset = object ? uintptr_t(object->address_begin) : 0;
        const void * physical_addr = reinterpret_cast<const void *>(uintptr_t(virtual_addr) - virtual_offset);

        out << i << ". ";

        if (object)
        {
            if (std::filesystem::exists(object->name))
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;

                DB::Dwarf::LocationInfo location;
                auto mode = fatal ? DB::Dwarf::LocationInfoMode::FULL_WITH_INLINE : DB::Dwarf::LocationInfoMode::FAST;
                if (dwarf_it->second.findAddress(uintptr_t(physical_addr), location, mode, inline_frames))
                    out << location.file.toString() << ":" << location.line << ": ";
            }
        }

        const auto * symbol = symbol_index.findSymbol(virtual_addr);
        if (symbol)
        {
            int status = 0;
            out << demangle(symbol->name, status);
        }
        else
            out << "?";

        if (show_addresses.load(std::memory_order_relaxed))
            out << " @ " << physical_addr;

        out << " in " << (object ? object->name : "?");

        for (size_t j = 0; j < inline_frames.size(); ++j)
        {
            const auto & frame = inline_frames[j];
            int status = 0;
            callback(fmt::format("{}.{}. inlined from {}:{}: {}",
                     i, j+1, frame.location.file.toString(), frame.location.line, demangle(frame.name, status)));
        }

        callback(out.str());
        out.str({});
    }
#else
    if (show_addresses.load(std::memory_order_relaxed))
    {
        std::stringstream out;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        out.exceptions(std::ios::failbit);

        for (size_t i = offset; i < size; ++i)
        {
            const void * addr = frame_pointers[i];
            out << i << ". " << addr;

            callback(out.str());
            out.str({});
        }
    }
    else
        callback("Addresses are hidden for security reasons.");
#endif
}

static std::string toStringImpl(const StackTrace::FramePointers & frame_pointers, size_t offset, size_t size)
{
    std::stringstream out;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out.exceptions(std::ios::failbit);
    toStringEveryLineImpl(false, frame_pointers, offset, size, [&](const std::string & str) { out << str << '\n'; });
    return out.str();
}

void StackTrace::toStringEveryLine(std::function<void(const std::string &)> callback) const
{
    toStringEveryLineImpl(true, frame_pointers, offset, size, std::move(callback));
}


std::string StackTrace::toString() const
{
    return toStringStatic(frame_pointers, offset, size);
}

std::string StackTrace::toString(void ** frame_pointers_, size_t offset, size_t size)
{
    __msan_unpoison(frame_pointers_, size * sizeof(*frame_pointers_));

    StackTrace::FramePointers frame_pointers_copy{};
    for (size_t i = 0; i < size; ++i)
        frame_pointers_copy[i] = frame_pointers_[i];

    return toStringStatic(frame_pointers_copy, offset, size);
}

static CachedFn<&toStringImpl> & cacheInstance()
{
    static CachedFn<&toStringImpl> cache;
    return cache;
}

std::string StackTrace::toStringStatic(const StackTrace::FramePointers & frame_pointers, size_t offset, size_t size)
{
    /// Calculation of stack trace text is extremely slow.
    /// We use simple cache because otherwise the server could be overloaded by trash queries.
    return cacheInstance()(frame_pointers, offset, size);
}

void StackTrace::dropCache()
{
    cacheInstance().drop();
}
