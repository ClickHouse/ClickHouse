// Copyright Vladimir Prus 2002-2004.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)


#define BOOST_PROGRAM_OPTIONS_SOURCE
#include <boost/program_options/config.hpp>

#include <boost/program_options/detail/config_file.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/detail/convert.hpp>
#include <boost/throw_exception.hpp>

#include <iostream>
#include <fstream>
#include <cassert>

namespace boost { namespace program_options { namespace detail {

    using namespace std;

    common_config_file_iterator::common_config_file_iterator(
        const std::set<std::string>& allowed_options,
        bool allow_unregistered)
    : allowed_options(allowed_options),
      m_allow_unregistered(allow_unregistered)
    {
        for(std::set<std::string>::const_iterator i = allowed_options.begin();
            i != allowed_options.end(); 
            ++i)
        {
            add_option(i->c_str());
        }
    }

    void
    common_config_file_iterator::add_option(const char* name)
    {
        string s(name);
        assert(!s.empty());
        if (*s.rbegin() == '*') {
            s.resize(s.size()-1);
            bool bad_prefixes(false);
            // If 's' is a prefix of one of allowed suffix, then
            // lower_bound will return that element.
            // If some element is prefix of 's', then lower_bound will
            // return the next element.
            set<string>::iterator i = allowed_prefixes.lower_bound(s);
            if (i != allowed_prefixes.end()) {
                if (i->find(s) == 0)
                    bad_prefixes = true;                    
            }
            if (i != allowed_prefixes.begin()) {
                --i;
                if (s.find(*i) == 0)
                    bad_prefixes = true;
            }
            if (bad_prefixes)
                boost::throw_exception(error("options '" + string(name) + "' and '" +
                                             *i + "*' will both match the same "
                                             "arguments from the configuration file"));
            allowed_prefixes.insert(s);
        }
    }

    namespace {
        string trim_ws(const string& s)
        {
            string::size_type n, n2;
            n = s.find_first_not_of(" \t\r\n");
            if (n == string::npos)
                return string();
            else {
                n2 = s.find_last_not_of(" \t\r\n");
                return s.substr(n, n2-n+1);
            }
        }
    }


    void common_config_file_iterator::get()
    {
        string s;
        string::size_type n;
        bool found = false;

        while(this->getline(s)) {

            // strip '#' comments and whitespace
            if ((n = s.find('#')) != string::npos)
                s = s.substr(0, n);
            s = trim_ws(s);

            if (!s.empty()) {
                // Handle section name
                if (*s.begin() == '[' && *s.rbegin() == ']') {
                    m_prefix = s.substr(1, s.size()-2);
                    if (*m_prefix.rbegin() != '.')
                        m_prefix += '.';
                }
                else if ((n = s.find('=')) != string::npos) {

                    string name = m_prefix + trim_ws(s.substr(0, n));
                    string value = trim_ws(s.substr(n+1));

                    bool registered = allowed_option(name);
                    if (!registered && !m_allow_unregistered)
                        boost::throw_exception(unknown_option(name));

                    found = true;
                    this->value().string_key = name;
                    this->value().value.clear();
                    this->value().value.push_back(value);
                    this->value().unregistered = !registered;
                    this->value().original_tokens.clear();
                    this->value().original_tokens.push_back(name);
                    this->value().original_tokens.push_back(value);
                    break;

                } else {
                    boost::throw_exception(invalid_config_file_syntax(s, invalid_syntax::unrecognized_line));
                }
            }
        }
        if (!found)
            found_eof();
    }


    bool 
    common_config_file_iterator::allowed_option(const std::string& s) const
    {
        set<string>::const_iterator i = allowed_options.find(s);
        if (i != allowed_options.end())
            return true;        
        // If s is "pa" where "p" is allowed prefix then
        // lower_bound should find the element after "p". 
        // This depends on 'allowed_prefixes' invariant.
        i = allowed_prefixes.lower_bound(s);
        if (i != allowed_prefixes.begin() && s.find(*--i) == 0)
            return true;
        return false;
    }

#if BOOST_WORKAROUND(__COMO_VERSION__, BOOST_TESTED_AT(4303)) || \
        (defined(__sgi) && BOOST_WORKAROUND(_COMPILER_VERSION, BOOST_TESTED_AT(741)))
    template<>
    bool
    basic_config_file_iterator<wchar_t>::getline(std::string& s)
    {
        std::wstring ws;
        // On Comeau, using two-argument version causes
        // call to some internal function with std::wstring, and '\n'
        // (not L'\n') and compile can't resolve that call.

        if (std::getline(*is, ws, L'\n')) {
            s = to_utf8(ws);
            return true;
        } else {
            return false;
        }            
    }
#endif    

}}}

#if 0
using boost::program_options::config_file;

#include <sstream>
#include <cassert>

int main()
{
    try {
        stringstream s(
            "a = 1\n"
            "b = 2\n");

        config_file cf(s);
        cf.add_option("a");
        cf.add_option("b");

        assert(++cf);
        assert(cf.name() == "a");
        assert(cf.value() == "1");
        assert(++cf);
        assert(cf.name() == "b");
        assert(cf.value() == "2");
        assert(!++cf);
    }
    catch(exception& e)
    {
        cout << e.what() << "\n";
    }
}
#endif
