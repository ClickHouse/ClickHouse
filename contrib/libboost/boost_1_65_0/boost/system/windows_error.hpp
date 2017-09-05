//  boost/system/windows_error.hpp  ------------------------------------------//

//  Copyright Beman Dawes 2007

//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See library home page at http://www.boost.org/libs/system

#ifndef BOOST_SYSTEM_WINDOWS_ERROR_HPP
#define BOOST_SYSTEM_WINDOWS_ERROR_HPP

//  This header is effectively empty for compiles on operating systems where
//  it is not applicable.

#include <boost/system/config.hpp>

#ifdef BOOST_WINDOWS_API

#include <boost/system/error_code.hpp>

//// Neither MinGW or Cygwin versions of winerror.h work if used alone, so on
//// either of those platforms include the full windows.h
//
//#if defined(__MINGW32__) || defined(__CYGWIN__)
//#include <windows.h>
//#else
//#include <winerror.h>
//#endif

#include <boost/detail/winapi/error_codes.hpp>

namespace boost
{
  namespace system
  {

    //  Microsoft Windows  ---------------------------------------------------//

    //  To construct an error_code after a API error:
    //
    //      error_code( ::GetLastError(), system_category() )

    namespace windows_error
    {
      enum windows_error_code
      {
        success = 0,
        // These names and values are based on Windows winerror.h
        invalid_function = boost::detail::winapi::ERROR_INVALID_FUNCTION_,
        file_not_found = boost::detail::winapi::ERROR_FILE_NOT_FOUND_,
        path_not_found = boost::detail::winapi::ERROR_PATH_NOT_FOUND_,
        too_many_open_files = boost::detail::winapi::ERROR_TOO_MANY_OPEN_FILES_,
        access_denied = boost::detail::winapi::ERROR_ACCESS_DENIED_,
        invalid_handle = boost::detail::winapi::ERROR_INVALID_HANDLE_,
        arena_trashed = boost::detail::winapi::ERROR_ARENA_TRASHED_,
        not_enough_memory = boost::detail::winapi::ERROR_NOT_ENOUGH_MEMORY_,
        invalid_block = boost::detail::winapi::ERROR_INVALID_BLOCK_,
        bad_environment = boost::detail::winapi::ERROR_BAD_ENVIRONMENT_,
        bad_format = boost::detail::winapi::ERROR_BAD_FORMAT_,
        invalid_access = boost::detail::winapi::ERROR_INVALID_ACCESS_,
        outofmemory = boost::detail::winapi::ERROR_OUTOFMEMORY_,
        invalid_drive = boost::detail::winapi::ERROR_INVALID_DRIVE_,
        current_directory = boost::detail::winapi::ERROR_CURRENT_DIRECTORY_,
        not_same_device = boost::detail::winapi::ERROR_NOT_SAME_DEVICE_,
        no_more_files = boost::detail::winapi::ERROR_NO_MORE_FILES_,
        write_protect = boost::detail::winapi::ERROR_WRITE_PROTECT_,
        bad_unit = boost::detail::winapi::ERROR_BAD_UNIT_,
        not_ready = boost::detail::winapi::ERROR_NOT_READY_,
        bad_command = boost::detail::winapi::ERROR_BAD_COMMAND_,
        crc = boost::detail::winapi::ERROR_CRC_,
        bad_length = boost::detail::winapi::ERROR_BAD_LENGTH_,
        seek = boost::detail::winapi::ERROR_SEEK_,
        not_dos_disk = boost::detail::winapi::ERROR_NOT_DOS_DISK_,
        sector_not_found = boost::detail::winapi::ERROR_SECTOR_NOT_FOUND_,
        out_of_paper = boost::detail::winapi::ERROR_OUT_OF_PAPER_,
        write_fault = boost::detail::winapi::ERROR_WRITE_FAULT_,
        read_fault = boost::detail::winapi::ERROR_READ_FAULT_,
        gen_failure = boost::detail::winapi::ERROR_GEN_FAILURE_,
        sharing_violation = boost::detail::winapi::ERROR_SHARING_VIOLATION_,
        lock_violation = boost::detail::winapi::ERROR_LOCK_VIOLATION_,
        wrong_disk = boost::detail::winapi::ERROR_WRONG_DISK_,
        sharing_buffer_exceeded = boost::detail::winapi::ERROR_SHARING_BUFFER_EXCEEDED_,
        handle_eof = boost::detail::winapi::ERROR_HANDLE_EOF_,
        handle_disk_full= boost::detail::winapi::ERROR_HANDLE_DISK_FULL_,
        rem_not_list = boost::detail::winapi::ERROR_REM_NOT_LIST_,
        dup_name = boost::detail::winapi::ERROR_DUP_NAME_,
        bad_net_path = boost::detail::winapi::ERROR_BAD_NETPATH_,
        network_busy = boost::detail::winapi::ERROR_NETWORK_BUSY_,
        // ...
        file_exists = boost::detail::winapi::ERROR_FILE_EXISTS_,
        cannot_make = boost::detail::winapi::ERROR_CANNOT_MAKE_,
        // ...
        broken_pipe = boost::detail::winapi::ERROR_BROKEN_PIPE_,
        open_failed = boost::detail::winapi::ERROR_OPEN_FAILED_,
        buffer_overflow = boost::detail::winapi::ERROR_BUFFER_OVERFLOW_,
        disk_full= boost::detail::winapi::ERROR_DISK_FULL_,
        // ...
        lock_failed = boost::detail::winapi::ERROR_LOCK_FAILED_,
        busy = boost::detail::winapi::ERROR_BUSY_,
        cancel_violation = boost::detail::winapi::ERROR_CANCEL_VIOLATION_,
        already_exists = boost::detail::winapi::ERROR_ALREADY_EXISTS_
        // ...

        // TODO: add more Windows errors
      };

    }  // namespace windows

# ifndef BOOST_SYSTEM_NO_DEPRECATED
    namespace windows = windows_error;
# endif

    template<> struct is_error_code_enum<windows_error::windows_error_code>
      { static const bool value = true; };

    namespace windows_error
    {
      inline error_code make_error_code( windows_error_code e )
        { return error_code( e, system_category() ); }
    }

  }  // namespace system
}  // namespace boost

#endif  // BOOST_WINDOWS_API

#endif  // BOOST_SYSTEM_WINDOWS_ERROR_HPP
