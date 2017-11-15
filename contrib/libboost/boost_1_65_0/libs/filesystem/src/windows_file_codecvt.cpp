//  filesystem windows_file_codecvt.cpp  -----------------------------------------//

//  Copyright Beman Dawes 2009

//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

//  Library home page: http://www.boost.org/libs/filesystem

//--------------------------------------------------------------------------------------// 

// define BOOST_FILESYSTEM_SOURCE so that <boost/system/config.hpp> knows
// the library is being built (possibly exporting rather than importing code)
#define BOOST_FILESYSTEM_SOURCE 

#ifndef BOOST_SYSTEM_NO_DEPRECATED 
# define BOOST_SYSTEM_NO_DEPRECATED
#endif

#include <boost/filesystem/config.hpp>
#include <cwchar>  // for mbstate_t

#ifdef BOOST_WINDOWS_API

#include "windows_file_codecvt.hpp"

// Versions of MinGW prior to GCC 4.6 requires this
#ifndef WINVER
# define WINVER 0x0500
#endif

#include <windows.h>

  std::codecvt_base::result windows_file_codecvt::do_in(
    std::mbstate_t &, 
    const char* from, const char* from_end, const char*& from_next,
    wchar_t* to, wchar_t* to_end, wchar_t*& to_next) const
  {
    UINT codepage = AreFileApisANSI() ? CP_ACP : CP_OEMCP;

    int count;
    if ((count = ::MultiByteToWideChar(codepage, MB_PRECOMPOSED, from,
      from_end - from, to, to_end - to)) == 0) 
    {
      return error;  // conversion failed
    }

    from_next = from_end;
    to_next = to + count;
    *to_next = L'\0';
    return ok;
 }

  std::codecvt_base::result windows_file_codecvt::do_out(
    std::mbstate_t &,
    const wchar_t* from, const wchar_t* from_end, const wchar_t*  & from_next,
    char* to, char* to_end, char* & to_next) const
  {
    UINT codepage = AreFileApisANSI() ? CP_ACP : CP_OEMCP;

    int count;
    if ((count = ::WideCharToMultiByte(codepage, WC_NO_BEST_FIT_CHARS, from,
      from_end - from, to, to_end - to, 0, 0)) == 0)
    {
      return error;  // conversion failed
    }

    from_next = from_end;
    to_next = to + count;
    *to_next = '\0';
    return ok;
  }

  # endif  // BOOST_WINDOWS_API

