// (C) Copyright Michael Glassford 2004.
// Use, modification and distribution are subject to the
// Boost Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <boost/detail/winapi/config.hpp>
#include <boost/thread/detail/config.hpp>


#if defined(BOOST_HAS_WINTHREADS) && defined(BOOST_THREAD_BUILD_DLL)

    #include <boost/thread/detail/tss_hooks.hpp>

    #include <windows.h>

    #if defined(__BORLANDC__)
        extern "C" BOOL WINAPI DllEntryPoint(HINSTANCE /*hInstance*/, DWORD dwReason, LPVOID /*lpReserved*/)
    #elif defined(_WIN32_WCE)
        extern "C" BOOL WINAPI DllMain(HANDLE /*hInstance*/, DWORD dwReason, LPVOID /*lpReserved*/)
    #else
        extern "C" BOOL WINAPI DllMain(HINSTANCE /*hInstance*/, DWORD dwReason, LPVOID /*lpReserved*/)
    #endif
    {
        switch(dwReason)
        {
            case DLL_PROCESS_ATTACH:
            {
                boost::on_process_enter();
                boost::on_thread_enter();
                break;
            }

            case DLL_THREAD_ATTACH:
            {
                boost::on_thread_enter();
                break;
            }

            case DLL_THREAD_DETACH:
            {
                boost::on_thread_exit();
                break;
            }

            case DLL_PROCESS_DETACH:
            {
                boost::on_thread_exit();
                boost::on_process_exit();
                break;
            }
        }

        return TRUE;
    }

namespace boost
{
    void tss_cleanup_implemented()
    {
        /*
        This function's sole purpose is to cause a link error in cases where
        automatic tss cleanup is not implemented by Boost.Threads as a
        reminder that user code is responsible for calling the necessary
        functions at the appropriate times (and for implementing an a
        tss_cleanup_implemented() function to eliminate the linker's
        missing symbol error).

        If Boost.Threads later implements automatic tss cleanup in cases
        where it currently doesn't (which is the plan), the duplicate
        symbol error will warn the user that their custom solution is no
        longer needed and can be removed.
        */
    }
}

#else //defined(BOOST_HAS_WINTHREADS) && defined(BOOST_THREAD_BUILD_DLL)

#ifdef _MSC_VER
// Prevent LNK4221 warning with link=static
namespace boost { namespace link_static_warning_inhibit {
    extern __declspec(dllexport) void foo() { }
} }
#endif

#endif //defined(BOOST_HAS_WINTHREADS) && defined(BOOST_THREAD_BUILD_DLL)
