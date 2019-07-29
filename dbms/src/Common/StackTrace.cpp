#include <common/SimpleCache.h>
#include <common/demangle.h>

#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <Common/Elf.h>

#include <sstream>
#include <filesystem>
#include <unordered_map>
#include <cstring>


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
#else
    return nullptr;
#endif
}

StackTrace::StackTrace()
{
    tryCapture();
}

StackTrace::StackTrace(const ucontext_t & signal_context)
{
    tryCapture();

    if (size == 0)
    {
        /// No stack trace was captured. At least we can try parsing caller address
        void * caller_address = getCallerAddress(signal_context);
        if (caller_address)
            frames[size++] = reinterpret_cast<void *>(caller_address);
    }
}

StackTrace::StackTrace(NoCapture)
{
}


#if USE_UNWIND
extern "C" int unw_backtrace(void **, int);
#endif


void StackTrace::tryCapture()
{
    size = 0;
#if USE_UNWIND
    size = unw_backtrace(frames.data(), capacity);
#endif
}

size_t StackTrace::getSize() const
{
    return size;
}

const StackTrace::Frames & StackTrace::getFrames() const
{
    return frames;
}

std::string StackTrace::toString() const
{
    /// Calculation of stack trace text is extremely slow.
    /// We use simple cache because otherwise the server could be overloaded by trash queries.

    static SimpleCache<decltype(StackTrace::toStringImpl), &StackTrace::toStringImpl> func_cached;
    return func_cached(frames, size);
}

std::string StackTrace::toStringImpl(const Frames & frames, size_t size)
{
    if (size == 0)
        return "<Empty trace>";

    const DB::SymbolIndex & symbol_index = DB::SymbolIndex::instance();
    std::unordered_map<std::string, DB::Dwarf> dwarfs;

    std::stringstream out;

    for (size_t i = 0; i < size; ++i)
    {
        out << "#" << i << " " << frames[i] << " ";
        auto symbol = symbol_index.findSymbol(frames[i]);
        if (symbol)
        {
            int status = 0;
            out << demangle(symbol->name, status);
        }
        else
            out << "?";

        out << " ";

        if (auto object = symbol_index.findObject(frames[i]))
        {
            if (std::filesystem::exists(object->name))
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, *object->elf).first;

                DB::Dwarf::LocationInfo location;
                if (dwarf_it->second.findAddress(uintptr_t(object->address_begin) + uintptr_t(frames[i]), location, DB::Dwarf::LocationInfoMode::FAST))
                    out << location.file.toString() << ":" << location.line;
                else
                    out << object->name;
            }
        }
        else
            out << "?";

        out << "\n";
    }

    return out.str();
}
