// Copyright Vladimir Prus 2004.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)

#define BOOST_PROGRAM_OPTIONS_SOURCE
#include <boost/program_options/config.hpp>

#include <boost/program_options/positional_options.hpp>

#include <boost/limits.hpp>

#include <cassert>

namespace boost { namespace program_options {

    positional_options_description::positional_options_description()
    {}

    positional_options_description&
    positional_options_description::add(const char* name, int max_count)
    {
        assert(max_count != -1 || m_trailing.empty());

        if (max_count == -1)
            m_trailing = name;
        else {
            m_names.resize(m_names.size() + max_count, name);
        }
        return *this;
    }

    unsigned
    positional_options_description::max_total_count() const
    {
        return m_trailing.empty() ? 
          static_cast<unsigned>(m_names.size()) : (std::numeric_limits<unsigned>::max)();
    }
    
    const std::string& 
    positional_options_description::name_for_position(unsigned position) const
    {
        assert(position < max_total_count());

        if (position < m_names.size())
            return m_names[position];
        else
            return m_trailing;
    }


}}

