// Copyright Vladimir Prus 2004.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)

#define BOOST_PROGRAM_OPTIONS_SOURCE
#include <boost/program_options/config.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/detail/convert.hpp>
#include <boost/program_options/detail/cmdline.hpp>
#include <set>

#include <cctype>

namespace boost { namespace program_options {

    using namespace std;


#ifndef BOOST_NO_STD_WSTRING
    namespace
    {
        std::string convert_value(const std::wstring& s)
        {
            try {
                return to_local_8_bit(s);
            }
            catch(const std::exception&) {
                return "<unrepresentable unicode string>";
            }
        }
    }
#endif

    void 
    value_semantic_codecvt_helper<char>::
    parse(boost::any& value_store, 
          const std::vector<std::string>& new_tokens,
          bool utf8) const
    {
        if (utf8) {
#ifndef BOOST_NO_STD_WSTRING
            // Need to convert to local encoding.
            std::vector<string> local_tokens;
            for (unsigned i = 0; i < new_tokens.size(); ++i) {
                std::wstring w = from_utf8(new_tokens[i]);
                local_tokens.push_back(to_local_8_bit(w));
            }
            xparse(value_store, local_tokens);
#else
            boost::throw_exception(
                std::runtime_error("UTF-8 conversion not supported."));
#endif
        } else {
            // Already in local encoding, pass unmodified
            xparse(value_store, new_tokens);
        }        
    }

#ifndef BOOST_NO_STD_WSTRING
    void 
    value_semantic_codecvt_helper<wchar_t>::
    parse(boost::any& value_store, 
          const std::vector<std::string>& new_tokens,
          bool utf8) const
    {
        std::vector<wstring> tokens;
        if (utf8) {
            // Convert from utf8
            for (unsigned i = 0; i < new_tokens.size(); ++i) {
                tokens.push_back(from_utf8(new_tokens[i]));
            }
               
        } else {
            // Convert from local encoding
            for (unsigned i = 0; i < new_tokens.size(); ++i) {
                tokens.push_back(from_local_8_bit(new_tokens[i]));
            }
        }      

        xparse(value_store, tokens);  
    }
#endif

    BOOST_PROGRAM_OPTIONS_DECL std::string arg("arg");

    std::string
    untyped_value::name() const
    {
        return arg;
    }
    
    unsigned 
    untyped_value::min_tokens() const
    {
        if (m_zero_tokens)
            return 0;
        else
            return 1;
    }

    unsigned 
    untyped_value::max_tokens() const
    {
        if (m_zero_tokens)
            return 0;
        else
            return 1;
    }


    void 
    untyped_value::xparse(boost::any& value_store,
                          const std::vector<std::string>& new_tokens) const
    {
        if (!value_store.empty()) 
            boost::throw_exception(
                multiple_occurrences());
        if (new_tokens.size() > 1)
            boost::throw_exception(multiple_values());
        value_store = new_tokens.empty() ? std::string("") : new_tokens.front();
    }

    BOOST_PROGRAM_OPTIONS_DECL typed_value<bool>*
    bool_switch()
    {
        return bool_switch(0);
    }

    BOOST_PROGRAM_OPTIONS_DECL typed_value<bool>*
    bool_switch(bool* v)
    {
        typed_value<bool>* r = new typed_value<bool>(v);
        r->default_value(0);
        r->zero_tokens();

        return r;
    }

    /* Validates bool value.
        Any of "1", "true", "yes", "on" will be converted to "1".<br>
        Any of "0", "false", "no", "off" will be converted to "0".<br>
        Case is ignored. The 'xs' vector can either be empty, in which
        case the value is 'true', or can contain explicit value.
    */
    BOOST_PROGRAM_OPTIONS_DECL void validate(any& v, const vector<string>& xs,
                       bool*, int)
    {
        check_first_occurrence(v);
        string s(get_single_string(xs, true));

        for (size_t i = 0; i < s.size(); ++i)
            s[i] = char(tolower(s[i]));

        if (s.empty() || s == "on" || s == "yes" || s == "1" || s == "true")
            v = any(true);
        else if (s == "off" || s == "no" || s == "0" || s == "false")
            v = any(false);
        else
            boost::throw_exception(invalid_bool_value(s));
    }

    // This is blatant copy-paste. However, templating this will cause a problem,
    // since wstring can't be constructed/compared with char*. We'd need to
    // create auxiliary 'widen' routine to convert from char* into 
    // needed string type, and that's more work.
#if !defined(BOOST_NO_STD_WSTRING)
    BOOST_PROGRAM_OPTIONS_DECL 
    void validate(any& v, const vector<wstring>& xs, bool*, int)
    {
        check_first_occurrence(v);
        wstring s(get_single_string(xs, true));

        for (size_t i = 0; i < s.size(); ++i)
            s[i] = wchar_t(tolower(s[i]));

        if (s.empty() || s == L"on" || s == L"yes" || s == L"1" || s == L"true")
            v = any(true);
        else if (s == L"off" || s == L"no" || s == L"0" || s == L"false")
            v = any(false);
        else
            boost::throw_exception(invalid_bool_value(convert_value(s)));
    }
#endif
    BOOST_PROGRAM_OPTIONS_DECL 
    void validate(any& v, const vector<string>& xs, std::string*, int)
    {
        check_first_occurrence(v);
        v = any(get_single_string(xs));
    }

#if !defined(BOOST_NO_STD_WSTRING)
    BOOST_PROGRAM_OPTIONS_DECL 
    void validate(any& v, const vector<wstring>& xs, std::string*, int)
    {
        check_first_occurrence(v);
        v = any(get_single_string(xs));
    }
#endif

    namespace validators {

        BOOST_PROGRAM_OPTIONS_DECL 
        void check_first_occurrence(const boost::any& value)
        {
            if (!value.empty())
                boost::throw_exception(
                    multiple_occurrences());
        }
    }


    invalid_option_value::
    invalid_option_value(const std::string& bad_value)
    : validation_error(validation_error::invalid_option_value)
    {
        set_substitute("value", bad_value);
    }

#ifndef BOOST_NO_STD_WSTRING
    invalid_option_value::
    invalid_option_value(const std::wstring& bad_value)
    : validation_error(validation_error::invalid_option_value)
    {
        set_substitute("value", convert_value(bad_value));
    }
#endif



    invalid_bool_value::
    invalid_bool_value(const std::string& bad_value)
    : validation_error(validation_error::invalid_bool_value)
    {
        set_substitute("value", bad_value);
    }






    error_with_option_name::error_with_option_name( const std::string& template_,
                                                  const std::string& option_name,
                                                  const std::string& original_token,
                                                  int option_style) : 
                                        error(template_),
                                        m_option_style(option_style),
                                        m_error_template(template_)
    {
        //                     parameter            |     placeholder               |   value
        //                     ---------            |     -----------               |   -----
        set_substitute_default("canonical_option",  "option '%canonical_option%'",  "option");
        set_substitute_default("value",             "argument ('%value%')",         "argument");
        set_substitute_default("prefix",            "%prefix%",                     "");
        m_substitutions["option"] = option_name;
        m_substitutions["original_token"] = original_token;
    }


    const char* error_with_option_name::what() const throw()
    {
        // will substitute tokens each time what is run()
        substitute_placeholders(m_error_template);

        return m_message.c_str();
    }

    void error_with_option_name::replace_token(const string& from, const string& to) const
    {
        for (;;)
        {
            std::size_t pos = m_message.find(from.c_str(), 0, from.length());
            // not found: all replaced
            if (pos == std::string::npos)
                return;
            m_message.replace(pos, from.length(), to);
        }
    }

    string error_with_option_name::get_canonical_option_prefix() const
    {
        switch (m_option_style)
        {
        case command_line_style::allow_dash_for_short:
            return "-";
        case command_line_style::allow_slash_for_short:
            return "/";
        case command_line_style::allow_long_disguise:
            return "-";
        case command_line_style::allow_long:
            return "--";
        case 0:
            return "";
        }
        throw std::logic_error("error_with_option_name::m_option_style can only be "
                               "one of [0, allow_dash_for_short, allow_slash_for_short, "
                               "allow_long_disguise or allow_long]");
    }


    string error_with_option_name::get_canonical_option_name() const
    {
        if (!m_substitutions.find("option")->second.length())
            return m_substitutions.find("original_token")->second;

        string original_token   = strip_prefixes(m_substitutions.find("original_token")->second);
        string option_name      = strip_prefixes(m_substitutions.find("option")->second);

        //  For long options, use option name
        if (m_option_style == command_line_style::allow_long        || 
             m_option_style == command_line_style::allow_long_disguise)
            return get_canonical_option_prefix() + option_name;

        //  For short options use first letter of original_token
        if (m_option_style && original_token.length())
            return get_canonical_option_prefix() + original_token[0];

        // no prefix
        return option_name;
    }


    void error_with_option_name::substitute_placeholders(const string& error_template) const
    {
        m_message = error_template;
        std::map<std::string, std::string> substitutions(m_substitutions);
        substitutions["canonical_option"]   = get_canonical_option_name();
        substitutions["prefix"]             = get_canonical_option_prefix();


        //
        //  replace placeholder with defaults if values are missing 
        // 
        for (map<string, string_pair>::const_iterator iter = m_substitution_defaults.begin();
              iter != m_substitution_defaults.end(); ++iter)
        {
            // missing parameter: use default
            if (substitutions.count(iter->first) == 0 ||
                substitutions[iter->first].length() == 0)
                replace_token(iter->second.first, iter->second.second);
        }


        //
        //  replace placeholder with values
        //  placeholder are denoted by surrounding '%'
        // 
        for (map<string, string>::iterator iter = substitutions.begin();
              iter != substitutions.end(); ++iter)
            replace_token('%' + iter->first + '%', iter->second);
    }


    void ambiguous_option::substitute_placeholders(const string& original_error_template) const
    {
        // For short forms, all alternatives must be identical, by
        //      definition, to the specified option, so we don't need to
        //      display alternatives
        if (m_option_style == command_line_style::allow_dash_for_short || 
            m_option_style == command_line_style::allow_slash_for_short)
        {
            error_with_option_name::substitute_placeholders(original_error_template);
            return;
        }


        string error_template  = original_error_template;
        // remove duplicates using std::set
        std::set<std::string>   alternatives_set (m_alternatives.begin(), m_alternatives.end());
        std::vector<std::string> alternatives_vec (alternatives_set.begin(), alternatives_set.end());

        error_template += " and matches ";
        // Being very cautious: should be > 1 alternative!
        if (alternatives_vec.size() > 1)
        {
            for (unsigned i = 0; i < alternatives_vec.size() - 1; ++i)
                error_template += "'%prefix%" + alternatives_vec[i] + "', ";
            error_template += "and ";
        }

        // there is a programming error if multiple options have the same name...
        if (m_alternatives.size() > 1 && alternatives_vec.size() == 1)
            error_template += "different versions of ";

        error_template += "'%prefix%" + alternatives_vec.back() + "'";


        // use inherited logic
        error_with_option_name::substitute_placeholders(error_template);
    }






    string 
    validation_error::get_template(kind_t kind)
    {
        // Initially, store the message in 'const char*' variable,
        // to avoid conversion to std::string in all cases.
        const char* msg;
        switch(kind)
        {
        case invalid_bool_value:
            msg = "the argument ('%value%') for option '%canonical_option%' is invalid. Valid choices are 'on|off', 'yes|no', '1|0' and 'true|false'";
            break;
        case invalid_option_value:
            msg = "the argument ('%value%') for option '%canonical_option%' is invalid";
            break;
        case multiple_values_not_allowed:
            msg = "option '%canonical_option%' only takes a single argument";
            break;
        case at_least_one_value_required:
            msg = "option '%canonical_option%' requires at least one argument";
            break;
        // currently unused
        case invalid_option:
            msg = "option '%canonical_option%' is not valid";
            break;
        default:
            msg = "unknown error";
        }
        return msg;
    }

}}
