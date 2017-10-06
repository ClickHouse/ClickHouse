// Copyright 2014 Renato Tegon Forti, Antony Polukhin.
// Copyright 2015-2017 Antony Polukhin.
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_DLL_RUNTIME_SYMBOL_INFO_HPP
#define BOOST_DLL_RUNTIME_SYMBOL_INFO_HPP

#include <boost/config.hpp>
#include <boost/predef/os.h>
#include <boost/predef/compiler/visualc.h>
#include <boost/dll/detail/aggressive_ptr_cast.hpp>
#if BOOST_OS_WINDOWS
#   include <boost/detail/winapi/dll.hpp>
#   include <boost/dll/detail/windows/path_from_handle.hpp>
#else
#   include <dlfcn.h>
#   include <boost/dll/detail/posix/program_location_impl.hpp>
#endif

#ifdef BOOST_HAS_PRAGMA_ONCE
# pragma once
#endif

/// \file boost/dll/runtime_symbol_info.hpp
/// \brief Provides methods for getting acceptable by boost::dll::shared_library location of symbol, source line or program.
namespace boost { namespace dll {

#if BOOST_OS_WINDOWS
namespace detail {
    inline boost::filesystem::path program_location_impl(boost::system::error_code& ec) {
        return boost::dll::detail::path_from_handle(NULL, ec);
    }
} // namespace detail
#endif

    /*!
    * On success returns full path and name to the binary object that holds symbol pointed by ptr_to_symbol.
    *
    * \param ptr_to_symbol Pointer to symbol which location is to be determined.
    * \param ec Variable that will be set to the result of the operation.
    * \return Path to the binary object that holds symbol or empty path in case error.
    * \throws std::bad_alloc in case of insufficient memory. Overload that does not accept boost::system::error_code also throws boost::system::system_error.
    *
    * \b Examples:
    * \code
    * int main() {
    *    dll::symbol_location_ptr(std::set_terminate(0));       // returns "/some/path/libmy_terminate_handler.so"
    *    dll::symbol_location_ptr(::signal(SIGSEGV, SIG_DFL));  // returns "/some/path/libmy_symbol_handler.so"
    * }
    * \endcode
    */
    template <class T>
    inline boost::filesystem::path symbol_location_ptr(T ptr_to_symbol, boost::system::error_code& ec) {
        BOOST_STATIC_ASSERT_MSG(boost::is_pointer<T>::value, "boost::dll::symbol_location_ptr works only with pointers! `ptr_to_symbol` must be a pointer");
        boost::filesystem::path ret;
        if (!ptr_to_symbol) {
            ec = boost::system::error_code(
                boost::system::errc::bad_address,
                boost::system::generic_category()
            );

            return ret;
        }
        ec.clear();

        const void* ptr = boost::dll::detail::aggressive_ptr_cast<const void*>(ptr_to_symbol);

#if BOOST_OS_WINDOWS
        boost::detail::winapi::MEMORY_BASIC_INFORMATION_ mbi;
        if (!boost::detail::winapi::VirtualQuery(ptr, &mbi, sizeof(mbi))) {
            ec = boost::dll::detail::last_error_code();
            return ret;
        }

        return boost::dll::detail::path_from_handle(reinterpret_cast<boost::detail::winapi::HMODULE_>(mbi.AllocationBase), ec);
#else
        Dl_info info;

        // Some of the libc headers miss `const` in `dladdr(const void*, Dl_info*)`
        const int res = dladdr(const_cast<void*>(ptr), &info);

        if (res) {
            ret = info.dli_fname;
        } else {
            boost::dll::detail::reset_dlerror();
            ec = boost::system::error_code(
                boost::system::errc::bad_address,
                boost::system::generic_category()
            );
        }

        return ret;
#endif
    }

    //! \overload symbol_location_ptr(const void* ptr_to_symbol, boost::system::error_code& ec)
    template <class T>
    inline boost::filesystem::path symbol_location_ptr(T ptr_to_symbol) {
        boost::filesystem::path ret;
        boost::system::error_code ec;
        ret = boost::dll::symbol_location_ptr(ptr_to_symbol, ec);

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::symbol_location_ptr(T ptr_to_symbol) failed");
        }

        return ret;
    }

    /*!
    * On success returns full path and name of the binary object that holds symbol.
    *
    * \tparam T Type of the symbol, must not be explicitly specified.
    * \param symbol Symbol which location is to be determined.
    * \param ec Variable that will be set to the result of the operation.
    * \return Path to the binary object that holds symbol or empty path in case error.
    * \throws std::bad_alloc in case of insufficient memory. Overload that does not accept boost::system::error_code also throws boost::system::system_error.
    *
    * \b Examples:
    * \code
    * int var;
    * void foo() {}
    *
    * int main() {
    *    dll::symbol_location(var);                     // returns program location
    *    dll::symbol_location(foo);                     // returns program location
    *    dll::symbol_location(std::cerr);               // returns location of libstdc++: "/usr/lib/x86_64-linux-gnu/libstdc++.so.6"
    *    dll::symbol_location(std::placeholders::_1);   // returns location of libstdc++: "/usr/lib/x86_64-linux-gnu/libstdc++.so.6"
    *    dll::symbol_location(std::puts);               // returns location of libc: "/lib/x86_64-linux-gnu/libc.so.6"
    * }
    * \endcode
    */
    template <class T>
    inline boost::filesystem::path symbol_location(const T& symbol, boost::system::error_code& ec) {
        ec.clear();
        return boost::dll::symbol_location_ptr(
            boost::dll::detail::aggressive_ptr_cast<const void*>(boost::addressof(symbol)),
            ec
        );
    }

#if BOOST_COMP_MSVC < BOOST_VERSION_NUMBER(14,0,0)
    // Without this MSVC 7.1 fails with:
    //  ..\boost\dll\runtime_symbol_info.hpp(133) : error C2780: 'filesystem::path dll::symbol_location(const T &)' : expects 1 arguments - 2 provided
    template <class T>
    inline boost::filesystem::path symbol_location(const T& symbol, const char* /*workaround*/ = 0)
#else
    //! \overload symbol_location(const T& symbol, boost::system::error_code& ec)
    template <class T>
    inline boost::filesystem::path symbol_location(const T& symbol)
#endif
    {
        boost::filesystem::path ret;
        boost::system::error_code ec;
        ret = boost::dll::symbol_location_ptr(
            boost::dll::detail::aggressive_ptr_cast<const void*>(boost::addressof(symbol)),
            ec
        );

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::symbol_location(const T& symbol) failed");
        }

        return ret;
    }

    /// @cond
    // We have anonymous namespace here to make sure that `this_line_location()` method is instantiated in
    // current translation unit and is not shadowed by instantiations from other units.
    namespace {
    /// @endcond

    /*!
    * On success returns full path and name of the binary object that holds the current line of code
    * (the line in which the `this_line_location()` method was called).
    *
    * \param ec Variable that will be set to the result of the operation.
    * \throws std::bad_alloc in case of insufficient memory. Overload that does not accept boost::system::error_code also throws boost::system::system_error.
    */
    static inline boost::filesystem::path this_line_location(boost::system::error_code& ec) {
        typedef boost::filesystem::path(func_t)(boost::system::error_code& );
        func_t& f = this_line_location;
        return boost::dll::symbol_location(f, ec);
    }

    //! \overload this_line_location(boost::system::error_code& ec)
    static inline boost::filesystem::path this_line_location() {
        boost::filesystem::path ret;
        boost::system::error_code ec;
        ret = this_line_location(ec);

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::this_line_location() failed");
        }

        return ret;
    }

    /// @cond
    } // anonymous namespace
    /// @endcond

    /*!
    * On success returns full path and name of the currently running program (the one which contains the `main()` function).
    * 
    * Return value can be used as a parameter for shared_library. See Tutorial "Linking plugin into the executable"
    * for usage example. Flag '-rdynamic' must be used when linking the plugin into the executable
    * on Linux OS.
    *
    * \param ec Variable that will be set to the result of the operation.
    * \throws std::bad_alloc in case of insufficient memory. Overload that does not accept boost::system::error_code also throws boost::system::system_error.
    */
    inline boost::filesystem::path program_location(boost::system::error_code& ec) {
        ec.clear();
        return boost::dll::detail::program_location_impl(ec);
    }

    //! \overload program_location(boost::system::error_code& ec) {
    inline boost::filesystem::path program_location() {
        boost::filesystem::path ret;
        boost::system::error_code ec;
        ret = boost::dll::detail::program_location_impl(ec);

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::program_location() failed");
        }

        return ret;
    }

}} // namespace boost::dll

#endif // BOOST_DLL_RUNTIME_SYMBOL_INFO_HPP

