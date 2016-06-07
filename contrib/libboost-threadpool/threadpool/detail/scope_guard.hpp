/*! \file
* \brief TODO.
*
* TODO. 
*
* Copyright (c) 2005-2007 Philipp Henkel
*
* Use, modification, and distribution are  subject to the
* Boost Software License, Version 1.0. (See accompanying  file
* LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
*
* http://threadpool.sourceforge.net
*
*/


#ifndef THREADPOOL_DETAIL_SCOPE_GUARD_HPP_INCLUDED
#define THREADPOOL_DETAIL_SCOPE_GUARD_HPP_INCLUDED



#include <boost/function.hpp>


namespace boost { namespace threadpool { namespace detail 
{

// TODO documentation
class scope_guard
: private boost::noncopyable
{
	function0<void> const m_function;
	bool                  m_is_active;

public:
	scope_guard(function0<void> const & call_on_exit)
	: m_function(call_on_exit)
	, m_is_active(true)
	{
	}

	~scope_guard()
	{
		if(m_is_active && m_function)
		{
			m_function();
		}
	}

	void disable()
	{
		m_is_active = false;
	}
};






} } } // namespace boost::threadpool::detail

#endif // THREADPOOL_DETAIL_SCOPE_GUARD_HPP_INCLUDED


