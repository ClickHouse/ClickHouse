//  boost/system/error_code.hpp  -------------------------------------------------------//

//  Copyright Beman Dawes 2006, 2007
//  Copyright Christoper Kohlhoff 2007

//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See library home page at http://www.boost.org/libs/system

#ifndef BOOST_SYSTEM_ERROR_CODE_HPP
#define BOOST_SYSTEM_ERROR_CODE_HPP

#include <boost/system/config.hpp>
#include <boost/cstdint.hpp>
#include <boost/assert.hpp>
#include <boost/noncopyable.hpp>
#include <boost/utility/enable_if.hpp>
#include <ostream>
#include <string>
#include <stdexcept>
#include <functional>

// TODO: undef these macros if not already defined
#include <boost/cerrno.hpp>

#if !defined(BOOST_POSIX_API) && !defined(BOOST_WINDOWS_API)
#  error BOOST_POSIX_API or BOOST_WINDOWS_API must be defined
#endif

#ifndef BOOST_NO_CXX11_HDR_SYSTEM_ERROR
#include <system_error>
#endif

#include <boost/config/abi_prefix.hpp> // must be the last #include

#ifndef BOOST_SYSTEM_NOEXCEPT
#define BOOST_SYSTEM_NOEXCEPT BOOST_NOEXCEPT
#endif

namespace boost
{
  namespace system
  {

    class error_code;         // values defined by the operating system
    class error_condition;    // portable generic values defined below, but ultimately
                              // based on the POSIX standard

    //  "Concept" helpers  -------------------------------------------------------------//

    template< class T >
    struct is_error_code_enum { static const bool value = false; };

    template< class T >
    struct is_error_condition_enum { static const bool value = false; };

    //  generic error_conditions  ------------------------------------------------------//

    namespace errc
    {
      enum errc_t
      {
        success = 0,
        address_family_not_supported = EAFNOSUPPORT,
        address_in_use = EADDRINUSE,
        address_not_available = EADDRNOTAVAIL,
        already_connected = EISCONN,
        argument_list_too_long = E2BIG,
        argument_out_of_domain = EDOM,
        bad_address = EFAULT,
        bad_file_descriptor = EBADF,
        bad_message = EBADMSG,
        broken_pipe = EPIPE,
        connection_aborted = ECONNABORTED,
        connection_already_in_progress = EALREADY,
        connection_refused = ECONNREFUSED,
        connection_reset = ECONNRESET,
        cross_device_link = EXDEV,
        destination_address_required = EDESTADDRREQ,
        device_or_resource_busy = EBUSY,
        directory_not_empty = ENOTEMPTY,
        executable_format_error = ENOEXEC,
        file_exists = EEXIST,
        file_too_large = EFBIG,
        filename_too_long = ENAMETOOLONG,
        function_not_supported = ENOSYS,
        host_unreachable = EHOSTUNREACH,
        identifier_removed = EIDRM,
        illegal_byte_sequence = EILSEQ,
        inappropriate_io_control_operation = ENOTTY,
        interrupted = EINTR,
        invalid_argument = EINVAL,
        invalid_seek = ESPIPE,
        io_error = EIO,
        is_a_directory = EISDIR,
        message_size = EMSGSIZE,
        network_down = ENETDOWN,
        network_reset = ENETRESET,
        network_unreachable = ENETUNREACH,
        no_buffer_space = ENOBUFS,
        no_child_process = ECHILD,
        no_link = ENOLINK,
        no_lock_available = ENOLCK,
        no_message_available = ENODATA,
        no_message = ENOMSG,
        no_protocol_option = ENOPROTOOPT,
        no_space_on_device = ENOSPC,
        no_stream_resources = ENOSR,
        no_such_device_or_address = ENXIO,
        no_such_device = ENODEV,
        no_such_file_or_directory = ENOENT,
        no_such_process = ESRCH,
        not_a_directory = ENOTDIR,
        not_a_socket = ENOTSOCK,
        not_a_stream = ENOSTR,
        not_connected = ENOTCONN,
        not_enough_memory = ENOMEM,
        not_supported = ENOTSUP,
        operation_canceled = ECANCELED,
        operation_in_progress = EINPROGRESS,
        operation_not_permitted = EPERM,
        operation_not_supported = EOPNOTSUPP,
        operation_would_block = EWOULDBLOCK,
        owner_dead = EOWNERDEAD,
        permission_denied = EACCES,
        protocol_error = EPROTO,
        protocol_not_supported = EPROTONOSUPPORT,
        read_only_file_system = EROFS,
        resource_deadlock_would_occur = EDEADLK,
        resource_unavailable_try_again = EAGAIN,
        result_out_of_range = ERANGE,
        state_not_recoverable = ENOTRECOVERABLE,
        stream_timeout = ETIME,
        text_file_busy = ETXTBSY,
        timed_out = ETIMEDOUT,
        too_many_files_open_in_system = ENFILE,
        too_many_files_open = EMFILE,
        too_many_links = EMLINK,
        too_many_symbolic_link_levels = ELOOP,
        value_too_large = EOVERFLOW,
        wrong_protocol_type = EPROTOTYPE
      };

    } // namespace errc

# ifndef BOOST_SYSTEM_NO_DEPRECATED
    namespace posix = errc;
    namespace posix_error = errc;
# endif

    template<> struct is_error_condition_enum<errc::errc_t>
      { static const bool value = true; };


    //  --------------------------------------------------------------------------------//

    //  Operating system specific interfaces  ------------------------------------------//


    //  The interface is divided into general and system-specific portions to
    //  meet these requirements:
    //
    //  * Code calling an operating system API can create an error_code with
    //    a single category (system_category), even for POSIX-like operating
    //    systems that return some POSIX errno values and some native errno
    //    values. This code should not have to pay the cost of distinguishing
    //    between categories, since it is not yet known if that is needed.
    //
    //  * Users wishing to write system-specific code should be given enums for
    //    at least the common error cases.
    //
    //  * System specific code should fail at compile time if moved to another
    //    operating system.

    //  The system specific portions of the interface are located in headers
    //  with names reflecting the operating system. For example,
    //
    //       <boost/system/cygwin_error.hpp>
    //       <boost/system/linux_error.hpp>
    //       <boost/system/windows_error.hpp>
    //
    //  These headers are effectively empty for compiles on operating systems
    //  where they are not applicable.

    //  --------------------------------------------------------------------------------//

    class error_category;

    //  predefined error categories  ---------------------------------------------------//

#ifdef BOOST_ERROR_CODE_HEADER_ONLY
    inline const error_category &  system_category() BOOST_SYSTEM_NOEXCEPT;
    inline const error_category &  generic_category() BOOST_SYSTEM_NOEXCEPT;
#else
    BOOST_SYSTEM_DECL const error_category &  system_category() BOOST_SYSTEM_NOEXCEPT;
    BOOST_SYSTEM_DECL const error_category &  generic_category() BOOST_SYSTEM_NOEXCEPT;
#endif
    //  deprecated synonyms ------------------------------------------------------------//

#ifndef BOOST_SYSTEM_NO_DEPRECATED
    inline const error_category &  get_system_category() { return system_category(); }
    inline const error_category &  get_generic_category() { return generic_category(); }
    inline const error_category &  get_posix_category() { return generic_category(); }
    static const error_category &  posix_category BOOST_ATTRIBUTE_UNUSED
      = generic_category();
    static const error_category &  errno_ecat     BOOST_ATTRIBUTE_UNUSED
      = generic_category();
    static const error_category &  native_ecat    BOOST_ATTRIBUTE_UNUSED
      = system_category();
#endif

    //  class error_category  ------------------------------------------------//

    class error_category : public noncopyable
    {
#ifndef BOOST_NO_CXX11_HDR_SYSTEM_ERROR

    private:

      class std_category: public std::error_category
      {
      private:

        boost::system::error_category const * pc_;

      public:

        explicit std_category( boost::system::error_category const * pc ): pc_( pc )
        {
        }

        virtual const char * name() const BOOST_NOEXCEPT
        {
          return pc_->name();
        }

        virtual std::string message( int ev ) const
        {
          return pc_->message( ev );
        }

        virtual std::error_condition default_error_condition( int ev ) const
          BOOST_NOEXCEPT;
        virtual bool equivalent( int code, const std::error_condition & condition ) const
          BOOST_NOEXCEPT;
        virtual bool equivalent( const std::error_code & code, int condition ) const
          BOOST_NOEXCEPT;
      };

      std_category std_cat_;

    public:

      error_category() BOOST_SYSTEM_NOEXCEPT: std_cat_( this ) {}

      operator std::error_category const & () const BOOST_SYSTEM_NOEXCEPT
      {
        // do not map generic to std::generic on purpose; occasionally,
        // there are two std::generic categories in a program, which leads
        // to error codes/conditions mysteriously not being equal to themselves
        return std_cat_;
      }

#else

    // to maintain ABI compatibility between 03 and 11,
    // define a class with the same layout

    private:

      class std_category
      {
      private:

        boost::system::error_category const * pc_;

      public:

        explicit std_category( boost::system::error_category const * pc ): pc_( pc )
        {
        }

        virtual ~std_category() {}

        virtual const char * name() const BOOST_NOEXCEPT
        {
          return pc_->name();
        }

        // we can't define message, because (1) it returns an std::string,
        // which can be different between 03 and 11, and (2) on mingw, there
        // are actually two `message` functions, not one, so it doesn't work
        // even if we do

        // neither can we define default_error_condition or equivalent

        // if these functions are called, it will crash, but that's still
        // better than the alternative of having the class layout change
      };

      std_category std_cat_;

    public:

      error_category() BOOST_SYSTEM_NOEXCEPT: std_cat_( this ) {}

#endif

    public:
      virtual ~error_category(){}

      virtual const char *     name() const BOOST_SYSTEM_NOEXCEPT = 0;
      virtual std::string      message( int ev ) const = 0;
      inline virtual error_condition  default_error_condition( int ev ) const 
        BOOST_SYSTEM_NOEXCEPT;
      inline virtual bool             equivalent( int code,
                                           const error_condition & condition ) const 
        BOOST_SYSTEM_NOEXCEPT;
      inline virtual bool             equivalent( const error_code & code,
                                           int condition ) const  BOOST_SYSTEM_NOEXCEPT;

      bool operator==(const error_category & rhs) const BOOST_SYSTEM_NOEXCEPT
        { return this == &rhs; }
      bool operator!=(const error_category & rhs) const BOOST_SYSTEM_NOEXCEPT
        { return this != &rhs; }
      bool operator<( const error_category & rhs ) const BOOST_SYSTEM_NOEXCEPT
        { return std::less<const error_category*>()( this, &rhs ); }
    };

    //  class error_condition  ---------------------------------------------------------//

    //  error_conditions are portable, error_codes are system or library specific

    class error_condition
    {
    public:

      // constructors:
      error_condition() BOOST_SYSTEM_NOEXCEPT : m_val(0), m_cat(&generic_category()) {}
      error_condition( int val, const error_category & cat ) BOOST_SYSTEM_NOEXCEPT
        : m_val(val), m_cat(&cat) {}

      template <class ErrorConditionEnum>
        error_condition(ErrorConditionEnum e,
          typename boost::enable_if<is_error_condition_enum<ErrorConditionEnum> >::type*
            = 0) BOOST_SYSTEM_NOEXCEPT
      {
        *this = make_error_condition(e);
      }

      // modifiers:

      void assign( int val, const error_category & cat ) BOOST_SYSTEM_NOEXCEPT
      {
        m_val = val;
        m_cat = &cat;
      }

      template<typename ErrorConditionEnum>
        typename boost::enable_if<is_error_condition_enum<ErrorConditionEnum>,
          error_condition>::type &
          operator=( ErrorConditionEnum val ) BOOST_SYSTEM_NOEXCEPT
      {
        *this = make_error_condition(val);
        return *this;
      }

      void clear() BOOST_SYSTEM_NOEXCEPT
      {
        m_val = 0;
        m_cat = &generic_category();
      }

      // observers:
      int                     value() const BOOST_SYSTEM_NOEXCEPT    { return m_val; }
      const error_category &  category() const BOOST_SYSTEM_NOEXCEPT { return *m_cat; }
      std::string             message() const  { return m_cat->message(value()); }

      typedef void (*unspecified_bool_type)();
      static void unspecified_bool_true() {}

      operator unspecified_bool_type() const BOOST_SYSTEM_NOEXCEPT  // true if error
      {
        return m_val == 0 ? 0 : unspecified_bool_true;
      }

      bool operator!() const BOOST_SYSTEM_NOEXCEPT  // true if no error
      {
        return m_val == 0;
      }

      // relationals:
      //  the more symmetrical non-member syntax allows enum
      //  conversions work for both rhs and lhs.
      inline friend bool operator==( const error_condition & lhs,
                                     const error_condition & rhs ) BOOST_SYSTEM_NOEXCEPT
      {
        return lhs.m_cat == rhs.m_cat && lhs.m_val == rhs.m_val;
      }

      inline friend bool operator<( const error_condition & lhs,
                                    const error_condition & rhs ) BOOST_SYSTEM_NOEXCEPT
        //  the more symmetrical non-member syntax allows enum
        //  conversions work for both rhs and lhs.
      {
        return lhs.m_cat < rhs.m_cat
          || (lhs.m_cat == rhs.m_cat && lhs.m_val < rhs.m_val);
      }

#ifndef BOOST_NO_CXX11_HDR_SYSTEM_ERROR

      operator std::error_condition () const BOOST_SYSTEM_NOEXCEPT
      {
        return std::error_condition( value(), category() );
      }

#endif

    private:
      int                     m_val;
      const error_category *  m_cat;

    };

    //  class error_code  --------------------------------------------------------------//

    //  We want error_code to be a value type that can be copied without slicing
    //  and without requiring heap allocation, but we also want it to have
    //  polymorphic behavior based on the error category. This is achieved by
    //  abstract base class error_category supplying the polymorphic behavior,
    //  and error_code containing a pointer to an object of a type derived
    //  from error_category.
    class error_code
    {
    public:

      // constructors:
      error_code() BOOST_SYSTEM_NOEXCEPT : m_val(0), m_cat(&system_category()) {}
      error_code( int val, const error_category & cat ) BOOST_SYSTEM_NOEXCEPT
        : m_val(val), m_cat(&cat) {}

      template <class ErrorCodeEnum>
        error_code(ErrorCodeEnum e,
          typename boost::enable_if<is_error_code_enum<ErrorCodeEnum> >::type* = 0)
          BOOST_SYSTEM_NOEXCEPT
      {
        *this = make_error_code(e);
      }

      // modifiers:
      void assign( int val, const error_category & cat ) BOOST_SYSTEM_NOEXCEPT
      {
        m_val = val;
        m_cat = &cat;
      }

      template<typename ErrorCodeEnum>
        typename boost::enable_if<is_error_code_enum<ErrorCodeEnum>, error_code>::type &
          operator=( ErrorCodeEnum val ) BOOST_SYSTEM_NOEXCEPT
      {
        *this = make_error_code(val);
        return *this;
      }

      void clear() BOOST_SYSTEM_NOEXCEPT
      {
        m_val = 0;
        m_cat = &system_category();
      }

      // observers:
      int                     value() const  BOOST_SYSTEM_NOEXCEPT   { return m_val; }
      const error_category &  category() const BOOST_SYSTEM_NOEXCEPT { return *m_cat; }
      error_condition         default_error_condition() const BOOST_SYSTEM_NOEXCEPT
        { return m_cat->default_error_condition(value()); }
      std::string             message() const  { return m_cat->message(value()); }

      typedef void (*unspecified_bool_type)();
      static void unspecified_bool_true() {}

      operator unspecified_bool_type() const  BOOST_SYSTEM_NOEXCEPT // true if error
      {
        return m_val == 0 ? 0 : unspecified_bool_true;
      }

      bool operator!() const  BOOST_SYSTEM_NOEXCEPT // true if no error
      {
        return m_val == 0;
      }

      // relationals:
      inline friend bool operator==( const error_code & lhs,
                                     const error_code & rhs ) BOOST_SYSTEM_NOEXCEPT
        //  the more symmetrical non-member syntax allows enum
        //  conversions work for both rhs and lhs.
      {
        return lhs.m_cat == rhs.m_cat && lhs.m_val == rhs.m_val;
      }

      inline friend bool operator<( const error_code & lhs,
                                    const error_code & rhs ) BOOST_SYSTEM_NOEXCEPT
        //  the more symmetrical non-member syntax allows enum
        //  conversions work for both rhs and lhs.
      {
        return lhs.m_cat < rhs.m_cat
          || (lhs.m_cat == rhs.m_cat && lhs.m_val < rhs.m_val);
      }

#ifndef BOOST_NO_CXX11_HDR_SYSTEM_ERROR

      operator std::error_code () const BOOST_SYSTEM_NOEXCEPT
      {
        return std::error_code( value(), category() );
      }

#endif

    private:
      int                     m_val;
      const error_category *  m_cat;

    };

    //  predefined error_code object used as "throw on error" tag
# ifndef BOOST_SYSTEM_NO_DEPRECATED
    BOOST_SYSTEM_DECL extern error_code throws;
# endif

    //  Moving from a "throws" object to a "throws" function without breaking
    //  existing code is a bit of a problem. The workaround is to place the
    //  "throws" function in namespace boost rather than namespace boost::system.

  }  // namespace system

  namespace detail { inline system::error_code * throws() { return 0; } }
    //  Misuse of the error_code object is turned into a noisy failure by
    //  poisoning the reference. This particular implementation doesn't
    //  produce warnings or errors from popular compilers, is very efficient
    //  (as determined by inspecting generated code), and does not suffer
    //  from order of initialization problems. In practice, it also seems
    //  cause user function error handling implementation errors to be detected
    //  very early in the development cycle.

  inline system::error_code & throws()
    { return *detail::throws(); }

  namespace system
  {
    //  non-member functions  ------------------------------------------------//

    inline bool operator!=( const error_code & lhs,
                            const error_code & rhs ) BOOST_SYSTEM_NOEXCEPT
    {
      return !(lhs == rhs);
    }

    inline bool operator!=( const error_condition & lhs,
                            const error_condition & rhs ) BOOST_SYSTEM_NOEXCEPT
    {
      return !(lhs == rhs);
    }

    inline bool operator==( const error_code & code,
                            const error_condition & condition ) BOOST_SYSTEM_NOEXCEPT
    {
      return code.category().equivalent( code.value(), condition )
        || condition.category().equivalent( code, condition.value() );
    }

    inline bool operator!=( const error_code & lhs,
                            const error_condition & rhs ) BOOST_SYSTEM_NOEXCEPT
    {
      return !(lhs == rhs);
    }

    inline bool operator==( const error_condition & condition,
                            const error_code & code ) BOOST_SYSTEM_NOEXCEPT
    {
      return condition.category().equivalent( code, condition.value() )
        || code.category().equivalent( code.value(), condition );
    }

    inline bool operator!=( const error_condition & lhs,
                            const error_code & rhs ) BOOST_SYSTEM_NOEXCEPT
    {
      return !(lhs == rhs);
    }

    // TODO: both of these may move elsewhere, but the LWG hasn't spoken yet.

    template <class charT, class traits>
    inline std::basic_ostream<charT,traits>&
      operator<< (std::basic_ostream<charT,traits>& os, error_code ec)
    {
      os << ec.category().name() << ':' << ec.value();
      return os;
    }

    inline std::size_t hash_value( const error_code & ec )
    {
      return static_cast<std::size_t>(ec.value())
        + reinterpret_cast<std::size_t>(&ec.category());
    }

    //  make_* functions for errc::errc_t  ---------------------------------------------//

    namespace errc
    {
      //  explicit conversion:
      inline error_code make_error_code( errc_t e ) BOOST_SYSTEM_NOEXCEPT
        { return error_code( e, generic_category() ); }

      //  implicit conversion:
      inline error_condition make_error_condition( errc_t e ) BOOST_SYSTEM_NOEXCEPT
        { return error_condition( e, generic_category() ); }
    }

    //  error_category default implementation  -----------------------------------------//

    error_condition error_category::default_error_condition( int ev ) const
      BOOST_SYSTEM_NOEXCEPT
    {
      return error_condition( ev, *this );
    }

    bool error_category::equivalent( int code,
      const error_condition & condition ) const BOOST_SYSTEM_NOEXCEPT
    {
      return default_error_condition( code ) == condition;
    }

    bool error_category::equivalent( const error_code & code,
      int condition ) const BOOST_SYSTEM_NOEXCEPT
    {
      return *this == code.category() && code.value() == condition;
    }

#ifndef BOOST_NO_CXX11_HDR_SYSTEM_ERROR

    inline std::error_condition error_category::std_category::default_error_condition(
      int ev ) const BOOST_NOEXCEPT
    {
      return pc_->default_error_condition( ev );
    }

    inline bool error_category::std_category::equivalent( int code,
      const std::error_condition & condition ) const BOOST_NOEXCEPT
    {
      if( condition.category() == *this )
      {
        boost::system::error_condition bn( condition.value(), *pc_ );
        return pc_->equivalent( code, bn );
      }
      else if( condition.category() == std::generic_category()
        || condition.category() == boost::system::generic_category() )
      {
        boost::system::error_condition bn( condition.value(),
          boost::system::generic_category() );

        return pc_->equivalent( code, bn );
      }
#ifndef BOOST_NO_RTTI
      else if( std_category const* pc2 = dynamic_cast< std_category const* >(
        &condition.category() ) )
      {
        boost::system::error_condition bn( condition.value(), *pc2->pc_ );
        return pc_->equivalent( code, bn );
      }
#endif
      else
      {
        return default_error_condition( code ) == condition;
      }
    }

    inline bool error_category::std_category::equivalent( const std::error_code & code,
      int condition ) const BOOST_NOEXCEPT
    {
      if( code.category() == *this )
      {
        boost::system::error_code bc( code.value(), *pc_ );
        return pc_->equivalent( bc, condition );
      }
      else if( code.category() == std::generic_category()
        || code.category() == boost::system::generic_category() )
      {
        boost::system::error_code bc( code.value(),
          boost::system::generic_category() );

        return pc_->equivalent( bc, condition );
      }
#ifndef BOOST_NO_RTTI
      else if( std_category const* pc2 = dynamic_cast< std_category const* >(
        &code.category() ) )
      {
        boost::system::error_code bc( code.value(), *pc2->pc_ );
        return pc_->equivalent( bc, condition );
      }
#endif
      else if( *pc_ == boost::system::generic_category() )
      {
        return std::generic_category().equivalent( code, condition );
      }
      else
      {
        return false;
      }
    }

#endif

  } // namespace system
} // namespace boost

#include <boost/config/abi_suffix.hpp> // pops abi_prefix.hpp pragmas

# ifdef BOOST_ERROR_CODE_HEADER_ONLY
#   include <boost/system/detail/error_code.ipp>
# endif

#endif // BOOST_SYSTEM_ERROR_CODE_HPP
