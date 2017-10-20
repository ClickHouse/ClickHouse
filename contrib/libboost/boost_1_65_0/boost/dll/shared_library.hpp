// Copyright 2014 Renato Tegon Forti, Antony Polukhin.
// Copyright 2015-2016 Antony Polukhin.
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_DLL_SHARED_LIBRARY_HPP
#define BOOST_DLL_SHARED_LIBRARY_HPP

/// \file boost/dll/shared_library.hpp
/// \brief Contains the boost::dll::shared_library class, core class for all the
/// DLL/DSO operations.

#include <boost/config.hpp>
#include <boost/predef/os.h>
#include <boost/utility/enable_if.hpp>
#include <boost/type_traits/is_member_pointer.hpp>
#include <boost/utility/explicit_operator_bool.hpp>
#include <boost/dll/detail/system_error.hpp>
#include <boost/dll/detail/aggressive_ptr_cast.hpp>

#if BOOST_OS_WINDOWS
#   include <boost/dll/detail/windows/shared_library_impl.hpp>
#else
#   include <boost/dll/detail/posix/shared_library_impl.hpp>
#endif

#ifdef BOOST_HAS_PRAGMA_ONCE
# pragma once
#endif

namespace boost { namespace dll {

/*!
* \brief This class can be used to load a
*        Dynamic link libraries (DLL's) or Shared Libraries, also know
*        as dynamic shared objects (DSO's) and get their exported
*        symbols (functions and variables).
*
* shared_library instances share reference count to an actual loaded DLL/DSO, so it
* is safe and memory efficient to have multiple instances of shared_library referencing the same DLL/DSO
* even if those instances were loaded using different paths (relative + absolute) referencing the same object.
*
* On Linux/POSIX link with library "dl". "-fvisibility=hidden" flag is also recommended for use on Linux/POSIX.
*/
class shared_library
/// @cond
    : private boost::dll::detail::shared_library_impl
/// @endcond
{
    typedef boost::dll::detail::shared_library_impl base_t;
    BOOST_COPYABLE_AND_MOVABLE(shared_library)

public:
#ifdef BOOST_DLL_DOXYGEN
    typedef platform_specific native_handle_t;
#else 
    typedef shared_library_impl::native_handle_t native_handle_t;
#endif

    /*!
    * Creates in anstance that does not reference any DLL/DSO.
    *
    * \post this->is_loaded() returns false.
    * \throw Nothing.
    */
    shared_library() BOOST_NOEXCEPT {}

    /*!
    * Copy constructor that increments the reference count of an underlying shared library.
    * Same as calling constructor with `lib.location()` parameter.
    *
    * \param lib A library to copy.
    * \post lib == *this
    * \throw boost::system::system_error, std::bad_alloc in case of insufficient memory.
    */
    shared_library(const shared_library& lib)
        : base_t()
    {
        assign(lib);
    }

    /*!
    * Copy constructor that increments the reference count of an underlying shared library.
    * Same as calling constructor with `lib.location(), ec` parameters.
    *
    * \param lib A shared library to copy.
    * \param ec Variable that will be set to the result of the operation.
    * \post lib == *this
    * \throw std::bad_alloc in case of insufficient memory.
    */
    shared_library(const shared_library& lib, boost::system::error_code& ec)
        : base_t()
    {
        assign(lib, ec);
    }

    /*!
    * Move constructor. Does not invalidate existing symbols and functions loaded from lib.
    *
    * \param lib A shared library to move from.
    * \post lib.is_loaded() returns false, this->is_loaded() return true.
    * \throw Nothing.
    */
    shared_library(BOOST_RV_REF(shared_library) lib) BOOST_NOEXCEPT
        : base_t(boost::move(static_cast<base_t&>(lib)))
    {}

    /*!
    * Loads a library by specified path with a specified mode.
    *
    * \param lib_path Library file name. Can handle std::string, const char*, std::wstring,
    *           const wchar_t* or boost::filesystem::path.
    * \param mode A mode that will be used on library load.
    * \throw boost::system::system_error, std::bad_alloc in case of insufficient memory.
    */
    explicit shared_library(const boost::filesystem::path& lib_path, load_mode::type mode = load_mode::default_mode) {
        shared_library::load(lib_path, mode);
    }

    /*!
    * Loads a library by specified path with a specified mode.
    *
    * \param lib_path Library file name. Can handle std::string, const char*, std::wstring,
    *           const wchar_t* or boost::filesystem::path.
    * \param mode A mode that will be used on library load.
    * \param ec Variable that will be set to the result of the operation.
    * \throw std::bad_alloc in case of insufficient memory.
    */
    shared_library(const boost::filesystem::path& lib_path, boost::system::error_code& ec, load_mode::type mode = load_mode::default_mode) {
        shared_library::load(lib_path, mode, ec);
    }

    //! \overload shared_library(const boost::filesystem::path& lib_path, boost::system::error_code& ec, load_mode::type mode = load_mode::default_mode)
    shared_library(const boost::filesystem::path& lib_path, load_mode::type mode, boost::system::error_code& ec) {
        shared_library::load(lib_path, mode, ec);
    }

    /*!
    * Assignment operator. If this->is_loaded() then calls this->unload(). Does not invalidate existing symbols and functions loaded from lib.
    *
    * \param lib A shared library to assign from.
    * \post lib == *this
    * \throw boost::system::system_error, std::bad_alloc in case of insufficient memory.
    */
    shared_library& operator=(BOOST_COPY_ASSIGN_REF(shared_library) lib) {
        boost::system::error_code ec;
        assign(lib, ec);
        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::shared_library::operator= failed");
        }

        return *this;
    }

    /*!
    * Move assignment operator. If this->is_loaded() then calls this->unload(). Does not invalidate existing symbols and functions loaded from lib.
    *
    * \param lib A library to move from.
    * \post lib.is_loaded() returns false.
    * \throw Nothing.
    */
    shared_library& operator=(BOOST_RV_REF(shared_library) lib) BOOST_NOEXCEPT {
        if (lib.native() != native()) {
            swap(lib);
        }

        return *this;
    }

    /*!
    * Destroys the object by calling `unload()`. If library was loaded multiple times
    * by different instances, the actual DLL/DSO won't be unloaded until
    * there is at least one instance that references the DLL/DSO.
    *
    * \throw Nothing.
    */
    ~shared_library() BOOST_NOEXCEPT {}

    /*!
    * Makes *this share the same shared object as lib. If *this is loaded, then unloads it.
    *
    * \post lib.location() == this->location(), lib == *this
    * \param lib A library to copy.
    * \param ec Variable that will be set to the result of the operation.
    * \throw std::bad_alloc in case of insufficient memory.
    */
    shared_library& assign(const shared_library& lib, boost::system::error_code& ec) {
        ec.clear();

        if (native() == lib.native()) {
            return *this;
        }

        if (!lib) {
            unload();
            return *this;
        }

        boost::filesystem::path loc = lib.location(ec);
        if (ec) {
            return *this;
        }

        shared_library copy(loc, ec);
        if (ec) {
            return *this;
        }

        swap(copy);
        return *this;
    }

    /*!
    * Makes *this share the same shared object as lib. If *this is loaded, then unloads it.
    *
    * \param lib A library instance to assign from.
    * \post lib.location() == this->location()
    * \throw boost::system::system_error, std::bad_alloc in case of insufficient memory.
    */
    shared_library& assign(const shared_library& lib) {
        boost::system::error_code ec;
        assign(lib, ec);
        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::shared_library::assign() failed");
        }

        return *this;
    }

    /*!
    * Loads a library by specified path with a specified mode.
    *
    * Note that if some library is already loaded in this instance, load will
    * call unload() and then load the new provided library.
    *
    * \param lib_path Library file name. Can handle std::string, const char*, std::wstring,
    *           const wchar_t* or boost::filesystem::path.
    * \param mode A mode that will be used on library load.
    * \throw boost::system::system_error, std::bad_alloc in case of insufficient memory.
    *
    */
    void load(const boost::filesystem::path& lib_path, load_mode::type mode = load_mode::default_mode) {
        boost::system::error_code ec;

        base_t::load(lib_path, mode, ec);

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::shared_library::load() failed");
        }
    }

    /*!
    * Loads a library by specified path with a specified mode.
    *
    * Note that if some library is already loaded in this instance, load will
    * call unload() and then load the new provided library.
    *
    * \param lib_path Library file name. Can handle std::string, const char*, std::wstring,
    *           const wchar_t* or boost::filesystem::path.
    * \param ec Variable that will be set to the result of the operation.
    * \param mode A mode that will be used on library load.
    * \throw std::bad_alloc in case of insufficient memory.
    */
    void load(const boost::filesystem::path& lib_path, boost::system::error_code& ec, load_mode::type mode = load_mode::default_mode) {
        ec.clear();
        base_t::load(lib_path, mode, ec);
    }

    //! \overload void load(const boost::filesystem::path& lib_path, boost::system::error_code& ec, load_mode::type mode = load_mode::default_mode)
    void load(const boost::filesystem::path& lib_path, load_mode::type mode, boost::system::error_code& ec) {
        ec.clear();
        base_t::load(lib_path, mode, ec);
    }

    /*!
    * Unloads a shared library.  If library was loaded multiple times
    * by different instances, the actual DLL/DSO won't be unloaded until
    * there is at least one instance that references the DLL/DSO.
    *
    * \post this->is_loaded() returns false.
    * \throw Nothing.
    */
    void unload() BOOST_NOEXCEPT {
        base_t::unload();
    }

    /*!
    * Check if an library is loaded.
    *
    * \return true if a library has been loaded.
    * \throw Nothing.
    */
    bool is_loaded() const BOOST_NOEXCEPT {
        return base_t::is_loaded();
    }

    /*!
    * Check if an library is not loaded.
    *
    * \return true if a library has not been loaded.
    * \throw Nothing.
    */
    bool operator!() const BOOST_NOEXCEPT {
        return !is_loaded();
    }

    /*!
    * Check if an library is loaded.
    *
    * \return true if a library has been loaded.
    * \throw Nothing.
    */
    BOOST_EXPLICIT_OPERATOR_BOOL()

    /*!
    * Search for a given symbol on loaded library. Works for all symbols, including alias names.
    *
    * \param symbol_name Null-terminated symbol name. Can handle std::string, char*, const char*.
    * \return `true` if the loaded library contains a symbol with a given name.
    * \throw Nothing.
    */
    bool has(const char* symbol_name) const BOOST_NOEXCEPT {
        boost::system::error_code ec;
        return is_loaded() && !!base_t::symbol_addr(symbol_name, ec) && !ec;
    }

    //! \overload bool has(const char* symbol_name) const
    bool has(const std::string& symbol_name) const BOOST_NOEXCEPT {
        return has(symbol_name.c_str());
    }

    /*!
    * Returns reference to the symbol (function or variable) with the given name from the loaded library.
    * This call will always succeed and throw nothing if call to `has(const char* )`
    * member function with the same symbol name returned `true`.
    *
    * \b Example:
    * \code
    * int& i0 = lib.get<int>("integer_name");
    * int& i1 = *lib.get<int*>("integer_alias_name");
    * \endcode
    *
    * \tparam T Type of the symbol that we are going to import. Must be explicitly specified.
    * \param symbol_name Null-terminated symbol name. Can handle std::string, char*, const char*.
    * \return Reference to the symbol.
    * \throw boost::system::system_error if symbol does not exist or if the DLL/DSO was not loaded.
    */
    template <typename T>
    inline typename boost::enable_if_c<boost::is_member_pointer<T>::value || boost::is_reference<T>::value, T>::type  get(const std::string& symbol_name) const {
        return get<T>(symbol_name.c_str());
    }

    //! \overload T& get(const std::string& symbol_name) const
    template <typename T>
    inline typename boost::disable_if_c<boost::is_member_pointer<T>::value || boost::is_reference<T>::value, T&>::type get(const std::string& symbol_name) const {
        return get<T>(symbol_name.c_str());
    }

    //! \overload T& get(const std::string& symbol_name) const
    template <typename T>
    inline typename boost::enable_if_c<boost::is_member_pointer<T>::value || boost::is_reference<T>::value, T>::type get(const char* symbol_name) const {
        return boost::dll::detail::aggressive_ptr_cast<T>(
            get_void(symbol_name)
        );
    }

    //! \overload T& get(const std::string& symbol_name) const
    template <typename T>
    inline typename boost::disable_if_c<boost::is_member_pointer<T>::value || boost::is_reference<T>::value, T&>::type get(const char* symbol_name) const {
        return *boost::dll::detail::aggressive_ptr_cast<T*>(
            get_void(symbol_name)
        );
    }

    /*!
    * Returns a symbol (function or variable) from a shared library by alias name of the symbol.
    *
    * \b Example:
    * \code
    * int& i = lib.get_alias<int>("integer_alias_name");
    * \endcode
    *
    * \tparam T Type of the symbol that we are going to import. Must be explicitly specified..
    * \param alias_name Null-terminated alias symbol name. Can handle std::string, char*, const char*.
    * \throw boost::system::system_error if symbol does not exist or if the DLL/DSO was not loaded.
    */
    template <typename T>
    inline T& get_alias(const char* alias_name) const {
        return *get<T*>(alias_name);
    }

    //! \overload T& get_alias(const char* alias_name) const
    template <typename T>
    inline T& get_alias(const std::string& alias_name) const {
        return *get<T*>(alias_name.c_str());
    }

private:
    /// @cond
    // get_void is required to reduce binary size: it does not depend on a template
    // parameter and will be instantiated only once.
    void* get_void(const char* sb) const {
        boost::system::error_code ec;

        if (!is_loaded()) {
            ec = boost::system::error_code(
                boost::system::errc::bad_file_descriptor,
                boost::system::generic_category()
            );

            // report_error() calls dlsym, do not use it here!
            boost::throw_exception(
                boost::system::system_error(
                    ec, "boost::dll::shared_library::get() failed: no library was loaded"
                )
            );
        }

        void* const ret = base_t::symbol_addr(sb, ec);
        if (ec || !ret) {
            boost::dll::detail::report_error(ec, "boost::dll::shared_library::get() failed");
        }

        return ret;
    }
    /// @endcond

public:

    /*!
    * Returns the native handler of the loaded library.
    *
    * \return Platform-specific handle.
    */
    native_handle_t native() const BOOST_NOEXCEPT {
        return base_t::native();
    }

   /*!
    * Returns full path and name of this shared object.
    *
    * \b Example:
    * \code
    * shared_library lib("test_lib.dll");
    * filesystem::path full_path = lib.location(); // C:\Windows\System32\test_lib.dll
    * \endcode
    *
    * \return Full path to the shared library.
    * \throw boost::system::system_error, std::bad_alloc.
    */
    boost::filesystem::path location() const {
        boost::system::error_code ec;
        if (!is_loaded()) {
            ec = boost::system::error_code(
                boost::system::errc::bad_file_descriptor,
                boost::system::generic_category()
            );

            boost::throw_exception(
                boost::system::system_error(
                    ec, "boost::dll::shared_library::location() failed (no library was loaded)"
                )
            );
        }

        boost::filesystem::path full_path = base_t::full_module_path(ec);

        if (ec) {
            boost::dll::detail::report_error(ec, "boost::dll::shared_library::location() failed");
        }

        return full_path;
    }

   /*!
    * Returns full path and name of shared module.
    *
    * \b Example:
    * \code
    * shared_library lib("test_lib.dll");
    * filesystem::path full_path = lib.location(); // C:\Windows\System32\test_lib.dll
    * \endcode
    *
    * \param ec Variable that will be set to the result of the operation.
    * \return Full path to the shared library.
    * \throw std::bad_alloc.
    */
    boost::filesystem::path location(boost::system::error_code& ec) const {
        if (!is_loaded()) {
            ec = boost::system::error_code(
                boost::system::errc::bad_file_descriptor,
                boost::system::generic_category()
            );

            return boost::filesystem::path();
        }

        ec.clear();
        return base_t::full_module_path(ec);
    }

    /*!
    * Returns suffix of shared module:
    * in a call to load() or the constructor/load.
    *
    * \return The suffix od shared module: ".dll" (Windows), ".so" (Unix/Linux/BSD), ".dylib" (MacOS/IOS)
    */
    static boost::filesystem::path suffix() {
        return base_t::suffix();
    }

    /*!
    * Swaps two libraries. Does not invalidate existing symbols and functions loaded from libraries.
    *
    * \param rhs Library to swap with.
    * \throw Nothing.
    */
    void swap(shared_library& rhs) BOOST_NOEXCEPT {
        base_t::swap(rhs);
    }
};



/// Very fast equality check that compares the actual DLL/DSO objects. Throws nothing.
inline bool operator==(const shared_library& lhs, const shared_library& rhs) BOOST_NOEXCEPT {
    return lhs.native() == rhs.native();
}

/// Very fast inequality check that compares the actual DLL/DSO objects. Throws nothing.
inline bool operator!=(const shared_library& lhs, const shared_library& rhs) BOOST_NOEXCEPT {
    return lhs.native() != rhs.native();
}

/// Compare the actual DLL/DSO objects without any guarantee to be stable between runs. Throws nothing.
inline bool operator<(const shared_library& lhs, const shared_library& rhs) BOOST_NOEXCEPT {
    return lhs.native() < rhs.native();
}

/// Swaps two shared libraries. Does not invalidate symbols and functions loaded from libraries. Throws nothing.
inline void swap(shared_library& lhs, shared_library& rhs) BOOST_NOEXCEPT {
    lhs.swap(rhs);
}

}} // boost::dll

#endif // BOOST_DLL_SHARED_LIBRARY_HPP

