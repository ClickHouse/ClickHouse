#include <common/Backtrace.h>
#include <common/SimpleCache.h>

#include <sstream>
#include <cstring>
#include <cxxabi.h>
#include <execinfo.h>

/// XXX: dbms depends on libcommon so we cannot add #include <Core/Defines.h> here
/// TODO: remove after libcommon and dbms merge
#if defined(__clang__)
    #define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#else
    #define NO_SANITIZE_ADDRESS
#endif

/// Arcadia compatibility DEVTOOLS-3976
#if defined(BACKTRACE_INCLUDE)
#include BACKTRACE_INCLUDE
#endif

#if !defined(BACKTRACE_FUNC) && USE_UNWIND
#define BACKTRACE_FUNC unw_backtrace
#endif

#if USE_UNWIND
#define UNW_LOCAL_ONLY
#include <libunwind.h>

static_assert(Backtrace::capacity < LIBUNWIND_MAX_STACK_SIZE, "Backtrace cannot be larger than libunwind upper bound on stack size");
#endif

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context)
{
    std::stringstream error;
    switch (sig)
    {
        case SIGSEGV:
        {
            /// Print info about address and reason.
            if (nullptr == info.si_addr)
                error << "Address: NULL pointer.";
            else
                error << "Address: " << info.si_addr;

#if defined(__x86_64__) && !defined(__FreeBSD__) && !defined(__APPLE__)
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
                    error << "Non-existant physical address.";
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
    }

    return error.str();
}

void * getCallerAddress(const ucontext_t & context)
{
#if defined(__x86_64__)
    /// Get the address at the time the signal was raised from the RIP (x86-64)
#if defined(__FreeBSD__)
    return reinterpret_cast<void *>(context.uc_mcontext.mc_rip);
#elif defined(__APPLE__)
    return reinterpret_cast<void *>(context.uc_mcontext->__ss.__rip);
#else
    return reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);
#endif
#elif defined(__aarch64__)
    return reinterpret_cast<void *>(context.uc_mcontext.pc);
#endif

    return nullptr;
}


Backtrace::Backtrace(std::optional<ucontext_t> signal_context)
{
    size = 0;

#if defined(BACKTRACE_FUNC)
    size = BACKTRACE_FUNC(frames.data(), capacity);
#else
    if (signal_context.has_value()) {
        /// No libunwind means no backtrace, because we are in a different thread from the one where the signal happened.
        /// So at least print the function where the signal happened.
        void * caller_address = getCallerAddress(*signal_context);
        if (caller_address)
            frames[size++] = reinterpret_cast<void *>(caller_address);
    }
#endif
}

Backtrace::Backtrace(const std::vector<void *>& sourceFrames) {
    for (size = 0; size < std::min(sourceFrames.size(), capacity); size++)
        frames[size] = sourceFrames[size];
}

size_t Backtrace::getSize() const {
    return size;
}

const Backtrace::Frames& Backtrace::getFrames() const {
    return frames;
}

std::string Backtrace::toString() const
{
    /// Calculation of stack trace text is extremely slow.
    /// We use simple cache because otherwise the server could be overloaded by trash queries.

    static SimpleCache<decltype(Backtrace::toStringImpl), &Backtrace::toStringImpl> func_cached;
    return func_cached(frames, size);
}

std::string Backtrace::toStringImpl(const Frames& frames, size_t size) {
    if (size == 0)
        return "<Empty trace>";

    std::stringstream backtrace;
    char ** symbols = backtrace_symbols(frames.data(), size);

    if (!symbols)
    {
        backtrace << "No symbols could be found for backtrace starting at " << frames[0];
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
        {
            /// Perform demangling of names. Name is in parentheses, before '+' character.

            char * name_start = nullptr;
            char * name_end = nullptr;
            char * demangled_name = nullptr;
            int status = 0;

            if (nullptr != (name_start = strchr(symbols[i], '('))
                && nullptr != (name_end = strchr(name_start, '+')))
            {
                ++name_start;
                *name_end = '\0';
                demangled_name = abi::__cxa_demangle(name_start, 0, 0, &status);
                *name_end = '+';
            }

            backtrace << i << ". ";

            if (nullptr != demangled_name && 0 == status)
            {
                backtrace.write(symbols[i], name_start - symbols[i]);
                backtrace << demangled_name << name_end;
            }
            else
                backtrace << symbols[i];

            backtrace << "\n";
        }
    }

    return backtrace.str();
}
