#include <Common/StackTrace.h>

#include <Core/Defines.h>
#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/SymbolIndex.h>
#include <Common/MemorySanitizer.h>
#include <common/SimpleCache.h>
#include <common/demangle.h>

#include <cstring>
#include <filesystem>
#include <sstream>
#include <unordered_map>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_UNWIND
#    include <libunwind.h>
#endif


namespace
{

void * getCallerAddress(const ucontext_t & context)
{
#if defined(__x86_64__)
    /// Get the address at the time the signal was raised from the RIP (x86-64)
#    if defined(__FreeBSD__)
    return reinterpret_cast<void *>(context.uc_mcontext.mc_rip);
#    elif defined(__APPLE__)
    return reinterpret_cast<void *>(context.uc_mcontext->__ss.__rip);
#    else
    return reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);
#    endif
#elif defined(__aarch64__)
    return reinterpret_cast<void *>(context.uc_mcontext.pc);
#else
    return nullptr;
#endif
}

void toStringEveryLineImpl(
    bool fast,
    const StackTrace::FramePointers & frame_pointers,
    size_t offset,
    size_t size,
    std::function<void(const std::string &)> callback)
{
    if (size == 0)
        return callback("<Empty trace>");

#if defined(__ELF__) && !defined(__FreeBSD__)
    auto symbol_index_ptr = DB::SymbolIndex::instance();
    const DB::SymbolIndex & symbol_index = *symbol_index_ptr;
    std::unordered_map<std::string, DB::Dwarf> dwarfs;

    std::stringstream out;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out.exceptions(std::ios::failbit);

    for (size_t i = offset; i < size; ++i)
    {
        const void * virtual_addr = frame_pointers[i];
        const auto * object = symbol_index.findObject(virtual_addr);
        uintptr_t virtual_offset = object ? uintptr_t(object->address_begin) : 0;
        const void * physical_addr = reinterpret_cast<const void *>(uintptr_t(virtual_addr) - virtual_offset);

        out << i << ". ";

        if (object)
        {
            if (std::filesystem::exists(object->name))
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, *object->elf).first;

                DB::Dwarf::LocationInfo location;
                if (dwarf_it->second.findAddress(
                        uintptr_t(physical_addr), location, fast ? DB::Dwarf::LocationInfoMode::FAST : DB::Dwarf::LocationInfoMode::FULL))
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

        out << " @ " << physical_addr;
        out << " in " << (object ? object->name : "?");

        callback(out.str());
        out.str({});
    }
#else
    std::stringstream out;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out.exceptions(std::ios::failbit);

    for (size_t i = offset; i < size; ++i)
    {
        const void * addr = frame_pointers[i];
        out << i << ". " << addr;

        callback(out.str());
        out.str({});
    }
#endif
}

std::string toStringImpl(const StackTrace::FramePointers & frame_pointers, size_t offset, size_t size)
{
    std::stringstream out;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out.exceptions(std::ios::failbit);
    toStringEveryLineImpl(true, frame_pointers, offset, size, [&](const std::string & str) { out << str << '\n'; });
    return out.str();
}

}  // namespace

StackTrace::StackTrace()
{
    tryCapture();
}

StackTrace::StackTrace(const ucontext_t & signal_context)
{
    tryCapture();

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

#if defined(STD_EXCEPTION_HAS_STACK_TRACE)
StackTrace::StackTrace(const std::exception & e) : size(e.get_stack_trace_size())
{
    // XXX: |std::exception::capacity| and |this->capacity| are out of sync
    for (auto i = 0u; i < size; ++i)
        frame_pointers[i] = e.get_stack_trace_frames()[i];
}
#endif

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

void StackTrace::toStringEveryLine(std::function<void(const std::string &)> callback) const
{
    toStringEveryLineImpl(false, frame_pointers, offset, size, std::move(callback));
}


std::string StackTrace::toString() const
{
    /// Calculation of stack trace text is extremely slow.
    /// We use simple cache because otherwise the server could be overloaded by trash queries.

    static SimpleCache<decltype(toStringImpl), &toStringImpl> func_cached;
    return func_cached(frame_pointers, offset, size);
}

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context)
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
            else
                error << "Address: " << info.si_addr;

#if defined(__x86_64__) && !defined(__FreeBSD__) && !defined(__APPLE__) && !defined(__arm__)
            auto err_mask = context.uc_mcontext.gregs[REG_ERR];
            if ((err_mask & 0x02))
                error << " Access: write.";
            else
                error << " Access: read.";
#else
            UNUSED(context);
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
