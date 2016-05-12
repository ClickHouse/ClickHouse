#pragma once

#include <boost/operators.hpp>

/** https://svn.boost.org/trac/boost/ticket/5182
  */

template <class T>
struct StrongTypedef
    : boost::totally_ordered1< StrongTypedef<T>
							   , boost::totally_ordered2< StrongTypedef<T>, T> >
{
	using Self = StrongTypedef<T>;
    T t;

    explicit StrongTypedef(const T t_) : t(t_) {};
    StrongTypedef(): t() {};
    StrongTypedef(const Self & t_) : t(t_.t){}
    Self & operator=(const Self & rhs) { t = rhs.t; return *this;}
    Self & operator=(const T & rhs) { t = rhs; return *this;}
    operator const T & () const {return t; }
    operator T & () { return t; }
    bool operator==(const Self & rhs) const { return t == rhs.t; }
    bool operator<(const Self & rhs) const { return t < rhs.t; }
    T & toUnderType() { return t; }
    const T & toUnderType() const { return t; }
};

namespace std
{
	template <class T>
	struct hash<StrongTypedef<T>>
	{
		size_t operator()(const StrongTypedef<T> & x) const
		{
			return std::hash<T>()(x.toUnderType());
		}
	};
}

#define STRONG_TYPEDEF(T, D) using D = StrongTypedef<T>;
