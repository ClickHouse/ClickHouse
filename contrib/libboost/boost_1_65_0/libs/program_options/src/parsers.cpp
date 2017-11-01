// Copyright Vladimir Prus 2002-2004.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)


#include <boost/config.hpp>

#define BOOST_PROGRAM_OPTIONS_SOURCE
#include <boost/program_options/config.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/detail/cmdline.hpp>
#include <boost/program_options/detail/config_file.hpp>
#include <boost/program_options/environment_iterator.hpp>
#include <boost/program_options/detail/convert.hpp>

#include <boost/bind.hpp>
#include <boost/throw_exception.hpp>

#include <cctype>
#include <fstream>

#if !defined(__GNUC__) || __GNUC__ < 3
#include <iostream>
#else
#include <istream>
#endif

#ifdef _WIN32
#include <stdlib.h>
#else
#include <unistd.h>
#endif

// The 'environ' should be declared in some cases. E.g. Linux man page says:
// (This variable must be declared in the user program, but is declared in 
// the header file unistd.h in case the header files came from libc4 or libc5, 
// and in case they came from glibc and _GNU_SOURCE was defined.) 
// To be safe, declare it here.

// It appears that on Mac OS X the 'environ' variable is not
// available to dynamically linked libraries.
// See: http://article.gmane.org/gmane.comp.lib.boost.devel/103843
// See: http://lists.gnu.org/archive/html/bug-guile/2004-01/msg00013.html
#if defined(__APPLE__) && defined(__DYNAMIC__)
// The proper include for this is crt_externs.h, however it's not
// available on iOS. The right replacement is not known. See
// https://svn.boost.org/trac/boost/ticket/5053
extern "C" { extern char ***_NSGetEnviron(void); }
#define environ (*_NSGetEnviron()) 
#else
#if defined(__MWERKS__)
#include <crtl.h>
#else
#if !defined(_WIN32) || defined(__COMO_VERSION__)
extern char** environ;
#endif
#endif
#endif

using namespace std;

namespace boost { namespace program_options {

#ifndef BOOST_NO_STD_WSTRING
    namespace {
        woption woption_from_option(const option& opt)
        {
            woption result;
            result.string_key = opt.string_key;
            result.position_key = opt.position_key;
            result.unregistered = opt.unregistered;
            
            std::transform(opt.value.begin(), opt.value.end(),
                           back_inserter(result.value),
                           boost::bind(from_utf8, _1));

            std::transform(opt.original_tokens.begin(), 
                           opt.original_tokens.end(),
                           back_inserter(result.original_tokens),
                           boost::bind(from_utf8, _1));
            return result;
        }
    }

    basic_parsed_options<wchar_t>
    ::basic_parsed_options(const parsed_options& po)
    : description(po.description),
      utf8_encoded_options(po),
      m_options_prefix(po.m_options_prefix)
    {
        for (unsigned i = 0; i < po.options.size(); ++i)
            options.push_back(woption_from_option(po.options[i]));
    }
#endif

    template<class charT>
    basic_parsed_options<charT>
    parse_config_file(std::basic_istream<charT>& is, 
                      const options_description& desc,
                      bool allow_unregistered)
    {    
        set<string> allowed_options;

        const vector<shared_ptr<option_description> >& options = desc.options();
        for (unsigned i = 0; i < options.size(); ++i)
        {
            const option_description& d = *options[i];

            if (d.long_name().empty())
                boost::throw_exception(
                    error("abbreviated option names are not permitted in options configuration files"));

            allowed_options.insert(d.long_name());
        }

        // Parser return char strings
        parsed_options result(&desc);        
        copy(detail::basic_config_file_iterator<charT>(
                 is, allowed_options, allow_unregistered), 
             detail::basic_config_file_iterator<charT>(), 
             back_inserter(result.options));
        // Convert char strings into desired type.
        return basic_parsed_options<charT>(result);
    }

    template
    BOOST_PROGRAM_OPTIONS_DECL basic_parsed_options<char>
    parse_config_file(std::basic_istream<char>& is, 
                      const options_description& desc,
                      bool allow_unregistered);

#ifndef BOOST_NO_STD_WSTRING
    template
    BOOST_PROGRAM_OPTIONS_DECL basic_parsed_options<wchar_t>
    parse_config_file(std::basic_istream<wchar_t>& is, 
                      const options_description& desc,
                      bool allow_unregistered);
#endif

    template<class charT>
    basic_parsed_options<charT>
    parse_config_file(const char* filename, 
                      const options_description& desc,
                      bool allow_unregistered)
    { 
        // Parser return char strings
        std::basic_ifstream< charT > strm(filename);
        if (!strm) 
        {
            boost::throw_exception(reading_file(filename));
        }
        return parse_config_file(strm, desc, allow_unregistered);
    }

    template
    BOOST_PROGRAM_OPTIONS_DECL basic_parsed_options<char>
    parse_config_file(const char* filename, 
                      const options_description& desc,
                      bool allow_unregistered);

#ifndef BOOST_NO_STD_WSTRING
    template
    BOOST_PROGRAM_OPTIONS_DECL basic_parsed_options<wchar_t>
    parse_config_file(const char* filename, 
                      const options_description& desc,
                      bool allow_unregistered);
#endif

    
// This versio, which accepts any options without validation, is disabled,
// in the hope that nobody will need it and we cant drop it altogether.
// Besides, probably the right way to handle all options is the '*' name.
#if 0
    BOOST_PROGRAM_OPTIONS_DECL parsed_options
    parse_config_file(std::istream& is)
    {
        detail::config_file_iterator cf(is, false);
        parsed_options result(0);
        copy(cf, detail::config_file_iterator(), 
             back_inserter(result.options));
        return result;
    }
#endif

    BOOST_PROGRAM_OPTIONS_DECL parsed_options
    parse_environment(const options_description& desc, 
                      const function1<std::string, std::string>& name_mapper)
    {
        parsed_options result(&desc);
        
        for(environment_iterator i(environ), e; i != e; ++i) {
            string option_name = name_mapper(i->first);

            if (!option_name.empty()) {
                option n;
                n.string_key = option_name;
                n.value.push_back(i->second);
                result.options.push_back(n);
            }                
        }

        return result;
    }

    namespace detail {
        class prefix_name_mapper {
        public:
            prefix_name_mapper(const std::string& prefix)
            : prefix(prefix)
            {}

            std::string operator()(const std::string& s)
            {
                string result;
                if (s.find(prefix) == 0) {
                    for(string::size_type n = prefix.size(); n < s.size(); ++n) 
                    {   
                        // Intel-Win-7.1 does not understand
            // push_back on string.         
                        result += static_cast<char>(tolower(s[n]));
                    }
                }
                return result;
            }
        private:
            std::string prefix;
        };
    }

    BOOST_PROGRAM_OPTIONS_DECL parsed_options
    parse_environment(const options_description& desc, 
                      const std::string& prefix)
    {
        return parse_environment(desc, detail::prefix_name_mapper(prefix));
    }

    BOOST_PROGRAM_OPTIONS_DECL parsed_options
    parse_environment(const options_description& desc, const char* prefix)
    {
        return parse_environment(desc, string(prefix));
    }




}}
