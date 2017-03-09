//  filesystem windows_file_codecvt.hpp  -----------------------------------------------//

//  Copyright Beman Dawes 2009

//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

//  Library home page: http://www.boost.org/libs/filesystem

#ifndef BOOST_FILESYSTEM3_WIN_FILE_CODECVT_HPP
#define BOOST_FILESYSTEM3_WIN_FILE_CODECVT_HPP

#include <boost/filesystem/config.hpp>
#include <locale>  

  //------------------------------------------------------------------------------------//
  //                                                                                    //
  //                          class windows_file_codecvt                                //
  //                                                                                    //
  //  Warning: partial implementation; even do_in and do_out only partially meet the    //
  //  standard library specifications as the "to" buffer must hold the entire result.   //
  //                                                                                    //
  //------------------------------------------------------------------------------------//

  class BOOST_FILESYSTEM_DECL windows_file_codecvt
    : public std::codecvt< wchar_t, char, std::mbstate_t >  
  {
  public:
    explicit windows_file_codecvt(std::size_t refs = 0)
        : std::codecvt<wchar_t, char, std::mbstate_t>(refs) {}
  protected:

    virtual bool do_always_noconv() const throw() { return false; }

    //  seems safest to assume variable number of characters since we don't
    //  actually know what codepage is active
    virtual int do_encoding() const throw() { return 0; }

    virtual std::codecvt_base::result do_in(std::mbstate_t& state, 
      const char* from, const char* from_end, const char*& from_next,
      wchar_t* to, wchar_t* to_end, wchar_t*& to_next) const;

    virtual std::codecvt_base::result do_out(std::mbstate_t & state,
      const wchar_t* from, const wchar_t* from_end, const wchar_t*& from_next,
      char* to, char* to_end, char*& to_next) const;

    virtual std::codecvt_base::result do_unshift(std::mbstate_t&,
        char* /*from*/, char* /*to*/, char* & /*next*/) const  { return ok; } 

    virtual int do_length(std::mbstate_t&,
      const char* /*from*/, const char* /*from_end*/, std::size_t /*max*/) const  { return 0; }

    virtual int do_max_length() const throw () { return 0; }
  };

#endif  // BOOST_FILESYSTEM3_WIN_FILE_CODECVT_HPP
