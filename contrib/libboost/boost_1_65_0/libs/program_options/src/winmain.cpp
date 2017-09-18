// Copyright Vladimir Prus 2002-2004.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)

#define BOOST_PROGRAM_OPTIONS_SOURCE
#include <boost/program_options/parsers.hpp>
#include <cctype>

using std::size_t;

#ifdef _WIN32
namespace boost { namespace program_options {

    // Take a command line string and splits in into tokens, according
    // to the rules windows command line processor uses.
    // 
    // The rules are pretty funny, see
    //    http://article.gmane.org/gmane.comp.lib.boost.user/3005
    //    http://msdn.microsoft.com/library/en-us/vccelng/htm/progs_12.asp
    BOOST_PROGRAM_OPTIONS_DECL
    std::vector<std::string> split_winmain(const std::string& input)
    {
        std::vector<std::string> result;

        std::string::const_iterator i = input.begin(), e = input.end();
        for(;i != e; ++i)
            if (!isspace((unsigned char)*i))
                break;
       
        if (i != e) {
   
            std::string current;
            bool inside_quoted = false;
            bool empty_quote = false;
            int backslash_count = 0;
            
            for(; i != e; ++i) {
                if (*i == '"') {
                    // '"' preceded by even number (n) of backslashes generates
                    // n/2 backslashes and is a quoted block delimiter
                    if (backslash_count % 2 == 0) {
                        current.append(backslash_count / 2, '\\');
                        empty_quote = inside_quoted && current.empty();
                        inside_quoted = !inside_quoted;
                        // '"' preceded by odd number (n) of backslashes generates
                        // (n-1)/2 backslashes and is literal quote.
                    } else {
                        current.append(backslash_count / 2, '\\');                
                        current += '"';                
                    }
                    backslash_count = 0;
                } else if (*i == '\\') {
                    ++backslash_count;
                } else {
                    // Not quote or backslash. All accumulated backslashes should be
                    // added
                    if (backslash_count) {
                        current.append(backslash_count, '\\');
                        backslash_count = 0;
                    }
                    if (isspace((unsigned char)*i) && !inside_quoted) {
                        // Space outside quoted section terminate the current argument
                        result.push_back(current);
                        current.resize(0);
                        empty_quote = false; 
                        for(;i != e && isspace((unsigned char)*i); ++i)
                            ;
                        --i;
                    } else {                  
                        current += *i;
                    }
                }
            }

            // If we have trailing backslashes, add them
            if (backslash_count)
                current.append(backslash_count, '\\');
        
            // If we have non-empty 'current' or we're still in quoted
            // section (even if 'current' is empty), add the last token.
            if (!current.empty() || inside_quoted || empty_quote)
                result.push_back(current);        
        }
        return result;
    }

#ifndef BOOST_NO_STD_WSTRING
    BOOST_PROGRAM_OPTIONS_DECL std::vector<std::wstring>
    split_winmain(const std::wstring& cmdline)
    {
        std::vector<std::wstring> result;
        std::vector<std::string> aux = split_winmain(to_internal(cmdline));
        for (size_t i = 0, e = aux.size(); i < e; ++i)
            result.push_back(from_utf8(aux[i]));
        return result;        
    }
#endif

}}
#endif

