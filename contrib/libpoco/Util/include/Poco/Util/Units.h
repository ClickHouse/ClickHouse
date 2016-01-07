//
// Units.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/Units.h#1 $
//
// Library: Util
// Package: Units
// Module:  Units
//
// Definitions for the C++ Units library.
//
// Copyright (c) 2007-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
// Adapted for POCO from the following source:
//
// C++ Units by Calum Grant
//
// Written by Calum Grant
// Copyright (C) Calum Grant 2007
//
// Home page: http://calumgrant.net/units
// File location: http://calumgrant.net/units/units.hpp
// Manual: http://calumgrant.net/units/units.html
//
// Copying permitted under the terms of the Boost software license.
//


#ifndef Util_Units_INCLUDED
#define Util_Units_INCLUDED


#include "Poco/Util/Util.h"
#include <cmath>


namespace Poco {
namespace Util {
namespace Units {


namespace Internal
{
	template <typename T1, typename T2> struct Convert;	
	struct None; 
	template <int Num, int Den, int Div=Num/Den, int Mod=Num%Den>
	struct FixedPower;
}


template <typename Unit1, typename Unit2>
struct Compose;
	/// Construct a unit equivalent to Unit1*Unit2


template <typename U, int Num, int Den = 1>
struct Scale;
	/// Constructs a unit equivalent to U*Num/Den


template <typename U, int Num, int Den = 1>
struct Translate;
	/// Constructs a Unit equivalent to U+Num/Den


template <typename U, int Num, int Den = 1>
struct Power;
	/// Constructs a Unit equivalent to U^(Num/Den)


typedef Power<Internal::None, 0> Unit;
	/// A unit which is effectively no units at all.


template <typename V, typename U>
class Value
	/// A Value with a unit.
	///	  - V is the type you are storing
	///   - U is the unit of the Value
	///
	/// This class is usually not used directly;
	/// client code should use the typedef'd 
	/// instantiations defined in the Values and
	/// Constants namespace.
	///
	/// Example:
	///
	///   using namespace Poco::Util::Units::Values;
	///
	///   std::cout << "One mile is " << km(mile(1)) << std::endl;
	///       // Output: One mile is 1.60934 km
	///
	///   std::cout << "Flow rate is " << m3(mile(1)*inch(80)*foot(9))/s(minute(5));
	///       // Output: Flow rate is 29.9026 (m)^3.(s)^-1
	///
	///   hours h;
	///   h = cm(3);   // Compile-time error: incompatible units
	///   h = 4;       // Compile-time error: 4 of what?
	///   h = days(4); // Ok: h is 96 hours
{
public:
	typedef V ValueType;
	typedef U Unit;

	Value(): _rep() 
	{ 
	}

	explicit Value(const ValueType& v): _rep(v) 
	{ 
	}
	
	template <typename OV, typename OU>
	Value(const Value<OV, OU>& v):
		_rep(Internal::Convert<OU, U>::fn(v.get()))
	{
	}

	const ValueType& get() const 
	{ 
		return _rep; 
	}
	 
	template <typename OV, typename OU>
	Value& operator = (const Value<OV, OU>& other)
	{
		_rep = Value(other).get();
		return *this;
	}

	template <typename OV, typename OU>
	Value operator + (const Value<OV, OU>& other) const
	{
		return Value(get() + Value(other).get());
	}

	template <typename OV, typename OU>
	Value& operator += (const Value<OV, OU>& other)
	{
		_rep += Value(other).get();
		return *this;
	}

	template <typename OV, typename OU>
	Value& operator -= (const Value<OV, OU>& other)
	{
		_rep -= Value(other).get();
		return *this;
	}

	template <typename OV, typename OU>
	Value operator - (const Value<OV, OU>& other) const
	{
		return Value(get() - Value(other).get());
	}

	Value operator - () const
	{
		return Value(-get());
	}

	template <typename OV, typename OU>
	Value<V, Compose<U, OU> >
		operator * (const Value<OV, OU>& other) const
	{
		return Value<V, Compose<U, OU> >(get() * other.get());
	}

	Value operator * (const ValueType& v) const
	{
		return Value(get() * v);
	}

	Value& operator *= (const ValueType& v)
	{
		_rep *= v;
		return *this;
	}

	template <typename OV, typename OU>
	Value<V, Compose<U, Power<OU, -1> > > operator / (const Value<OV, OU>& other) const
	{
		return Value<V, Compose<U, Power<OU, -1> > >(get() / other.get());
	}

	Value operator / (const ValueType& v) const
	{
		return Value(get() / v);
	}

	Value& operator /= (const ValueType& v)
	{
		_rep /= v;
		return *this;
	}

	template <typename OV, typename OU>
	bool operator == (const Value<OV, OU>& other) const
	{
		return get() == Value(other).get();
	}

	template <typename OV, typename OU>
	bool operator != (const Value<OV, OU>& other) const
	{
		return get() != Value(other).get();
	}

	template <typename OV, typename OU>
	bool operator < (const Value<OV, OU>& other) const
	{
		return get() < Value(other).get();
	}

	template <typename OV, typename OU>
	bool operator <= (const Value<OV, OU>& other) const
	{
		return get() <= Value(other).get();
	}

	template <typename OV, typename OU>
	bool operator > (const Value<OV, OU>& other) const
	{
		return get() > Value(other).get();
	}

	template <typename OV, typename OU>
	bool operator >= (const Value<OV, OU>& other) const
	{
		return get() >= Value(other).get();
	}

	Value& operator ++ () 
	{ 
		++_rep; 
		return *this; 
	}

	Value operator ++ (int) 
	{ 
		Value v = *this; 
		++_rep; 
		return v; 
	}

	Value& operator -- () 
	{ 
		--_rep; 
		return *this; 
	}

	Value operator -- (int) 
	{ 
		Value v = *this; 
		--_rep; 
		return v; 
	}

private:
	ValueType _rep;
};


template <typename V, typename U>
Value<V, Power<U, -1> > operator / (const V& a, const Value<V, U>& b)
{
	return Value<V, Power<U, -1> >(a / b.get());
}


template <typename V, typename U>
Value<V, U > operator * (const V& a, const Value<V, U>& b)
{
	return Value<V, U>(a * b.get());
}


template <typename V, typename U>
Value<V, Power<U, 1, 2> > sqrt(const Value<V, U>& a)
{
	return Value<V, Power<U, 1, 2> >(std::sqrt(a.get()));
}


template <typename V, typename U>
Value<V, Power<U, 2, 1> > square(const Value<V, U>& a)
{
	return Value<V, Power<U, 2, 1> >(std::pow(a.get(), 2));
}


template <typename V, typename U>
Value<V, Power<U, 3, 1> > cube(const Value<V, U>& a)
{
	return Value<V, Power<U, 3, 1> >(std::pow(a.get(), 3));
}


template <int Num, int Den, typename V, typename U>
Value<V, Power<U, Num, Den> > raise(const Value<V, U>& a)
{
	return Value<V, Power<U, Num, Den> >(Internal::FixedPower<Num, Den>::Power(a.get()));
}


namespace Internal
{
	template <typename T1, typename T2>
	struct Convertible;

	template <typename U>
	struct ScalingFactor;

	template <typename T1, typename T2> 
	struct Convert3
		/// Converts T1 to T2.
		/// Stage 3 - performed after Stage 1 and Stage 2.
		/// The reason we perform Convert in stages is so that the compiler
		/// can resolve templates in the order we want it to.
	{
		template <typename V>
		static V fn(const V& v)
			/// The default implementation assumes that the two quantities are in compatible
			/// Units up to some scaling factor.  Find the scaling factor and apply it.
		{
			return v * ScalingFactor<T2>::template fn<V>() / ScalingFactor<T1>::template fn<V>();
		}
	};

	template <typename T1, typename T2> 
	struct Convert2
		/// Converts T1 to T2.
		/// Template matches the first argument (T1),
		/// this is the fall-through to Convert3.
	{
		template <typename V>
		static V fn(const V& v)
		{
			return Convert3<T1,T2>::fn(v);
		}
	};

	template <typename T1, typename T2> 
	struct Convert
		/// Converts T1 to T2.
		/// If you really want to implement your own conversion routine,
		/// specialize this template.
		/// The default implementation falls through to Convert2.
	{
		/// If this fails, then T1 is not Convertible to T2:
		poco_static_assert ((Convertible<T1,T2>::Value));

		template <typename V>
		static V fn(const V& v)
		{
			return Convert2<T1,T2>::fn(v);
		}
	};

	template <typename T>
	struct Convert<T, T>
		// Trivial conversion to the same type.
	{
		template <typename U>
		static const U& fn(const U& u) { return u; }
	};
	
	template <typename T>
	struct Convert3<T, T>
		// Convert to same type.
	{
		template <typename U>
		static const U& fn(const U& u) { return u; }
	};

	template <typename T, typename U, int Num, int Den>
	struct Convert2<Scale<T, Num, Den>, U>
		// Convert from a scaled Unit.
	{
		template <typename V>
		static V fn(const V& v)
		{
			return Convert<T, U>::fn((v * Den)/Num);
		}
	};
	
	template <typename T, typename U, int Num, int Den>
	struct Convert3<T, Scale<U, Num, Den> >
		// Convert to a scaled Unit.
	{
		template <typename V>
		static V fn(const V& v)
		{
			return (Convert<T, U>::fn(v) * Num)/ Den;
		}
	};

	template <typename T, typename U, int Num, int Den>
	struct Convert2<Translate<T, Num, Den>, U>
		// Convert from a translated Unit.
	{
		template <typename V>
		static V fn(const V& v)
		{
			return Convert<T, U>::fn(v - static_cast<V>(Num) / static_cast<V>(Den));
		}
	};

	template <typename T, typename U, int Num, int Den>
	struct Convert3<T, Translate<U, Num, Den> >
		// Convert to a translated Unit.
	{
		template <typename V>
		static V fn(const V& v)
		{
			return Convert<T, U>::fn(v) + static_cast<V>(Num) / static_cast<V>(Den);
		}
	};

	template <typename Term, typename List>
	struct CountTerms
		/// Count the power to which Unit Term is raised in the Unit List.
		/// Returns a rational num/den of the power of term Term in List.
		/// The default assumes that Term is not found (num/den=0).
	{
		static const int num = 0;
		static const int den = 1;
	};

	template <typename Term>
	struct CountTerms<Term, Term>
	{
		static const int num = 1;
		static const int den = 1;
	}; 
 
	template <typename Term, typename U, int N, int D>
	struct CountTerms<Term, Scale<U, N, D> >
		// CountTerms ignores scaling factors - that is taken care of by ScalingFactor.
	{
		typedef CountTerms<Term, U> result;
		static const int num = result::num;
		static const int den = result::den;
	};

	template <typename Term, typename U, int N, int D>
	struct CountTerms<Term, Translate<U, N, D> >
		// CountTerms ignores translation.
	{
		typedef CountTerms<Term, U> result;
		static const int num = result::num;
		static const int den = result::den;
	};

	template <typename Term, typename T1, typename T2>
	struct CountTerms<Term, Compose<T1,T2> >
		// Addition of fractions.
	{
		typedef CountTerms<Term, T1> result1;
		typedef CountTerms<Term, T2> result2;
		static const int num = result1::num * result2::den + result1::den * result2::num;
		static const int den = result1::den * result2::den;
	};

	template <typename Term, typename U, int N, int D>
	struct CountTerms<Term, Power<U, N, D> >
		// Multiplication of fractions.
	{
		typedef CountTerms<Term, U> result;
		static const int num = N * result::num;
		static const int den = D * result::den;
	};

	template <typename Term, typename T1, typename T2>
	struct CheckTermsEqual
		/// Counts the power of the Unit Term in Units T1 and T2.
		/// Reports if they are equal, using equality of fractions.
		/// Does a depth-first search of the Unit "Term", 
		/// or counts the terms in the default case.
	{
		typedef CountTerms<Term, T1> count1;
		typedef CountTerms<Term, T2> count2;

		static const bool Value =
			count1::num * count2::den ==
			count1::den * count2::num;
	};

	template <typename U, int N, int D, typename T1, typename T2>
	struct CheckTermsEqual<Power<U, N, D>, T1, T2 >
	{
		static const bool Value = CheckTermsEqual<U, T1, T2>::Value;
	};

	template <typename U, int N, int D, typename T1, typename T2>
	struct CheckTermsEqual<Scale<U, N, D>, T1, T2 >
	{
		static const bool Value = CheckTermsEqual<U, T1, T2>::Value;
	};

	template <typename U, int N, int D, typename T1, typename T2>
	struct CheckTermsEqual<Translate<U, N, D>, T1, T2 >
	{
		static const bool Value = CheckTermsEqual<U, T1, T2>::Value;
	};

	template <typename T1, typename T2, typename T3, typename T4>
	struct CheckTermsEqual<Compose<T1,T2>,T3,T4>
	{
		static const bool Value =
			CheckTermsEqual<T1, T3, T4>::Value &&
			CheckTermsEqual<T2, T3, T4>::Value;
	};

	template <typename T1, typename T2>
	struct Convertible
		/// Determines whether two types are Convertible.
		/// Counts the powers in the LHS and RHS and ensures they are equal.
	{
		static const bool Value =
			CheckTermsEqual<T1,T1,T2>::Value &&
			CheckTermsEqual<T2,T1,T2>::Value;
	};

	template <int Num, int Den, int Div, int Mod>
	struct FixedPower
		/// A functor that raises a Value to the power Num/Den.
		/// The template is specialised for efficiency 
		/// so that we don't always have to call the std::power function.
	{
		template <typename T> static T Power(const T& t)
		{
			return std::pow(t, static_cast<T>(Num)/static_cast<T>(Den));
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, 1, 0>
	{
		template <typename T> static const T& Power(const T& t)
		{
			return t;
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, 2, 0>
	{
		template <typename T> static T Power(const T& t)
		{
			return t*t;
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, 3, 0>
	{
		template <typename T> static T Power(const T& t)
		{
			return t*t*t;
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, 4, 0>
	{
		template <typename T> static const T& Power(const T& t)
		{
			T u = t*t;
			return u*u;
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, -1, 0>
	{
		template <typename T> static T Power(const T& t)
		{
			return 1/t;
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, -2, 0>
	{
		template <typename T> static T Power(const T& t)
		{
			return 1/(t*t);
		}
	};

	template <int N, int D>
	struct FixedPower<N, D, 0, 0>
	{
		template <typename T> static T Power(const T& t)
		{
			return 1;
		}
	};

	template <typename U>
	struct ScalingFactor
		/// Determine the scaling factor of a Unit in relation to its "base" Units.
		/// Default is that U is a primitive Unit and is not scaled.
	{
		template <typename T>
		static T fn() { return 1; }
	};

	template <typename U1, typename U2>
	struct ScalingFactor< Compose<U1, U2> >
	{
		template <typename T>
		static T fn()
		{
			return
				ScalingFactor<U1>::template fn<T>() *
				ScalingFactor<U2>::template fn<T>();
		}
	};

	template <typename U, int N, int D>
	struct ScalingFactor< Scale<U, N, D> >
	{
		template <typename T>
		static T fn()
		{
			return
				ScalingFactor<U>::template fn<T>() *
				static_cast<T>(N) / static_cast<T>(D);
		}
	};

	template <typename U, int N, int D>
	struct ScalingFactor< Power<U, N, D> >
	{
		template <typename T>
		static T fn()
		{
			return FixedPower<N, D>::Power(ScalingFactor<U>::template fn<T>());
		}
	};

	template <typename U, int N, int D>
	struct ScalingFactor< Translate<U, N, D> >
	{
		template <typename T>
		static T fn()
		{
			return ScalingFactor<U>::template fn<T>();
		}
	};
} // namespace Internal


///
/// Display
///


#define UNIT_DISPLAY_NAME(Unit, string) \
	template <> \
	struct OutputUnit<Unit> \
	{ \
		template <typename Stream> \
		static void fn(Stream& os) \
		{ \
			os << string; \
		} \
	}


namespace Internal
{
	template <typename U>
	struct OutputUnit2
		/// The default Unit formatting mechanism.
	{
		template <typename Stream>
		static void fn(Stream &os) 
		{
			os << "Units";
		}
	};
}


template <typename U>
struct OutputUnit
	/// Functor to write Unit text to stream.
{
	template <typename Stream>
	static void fn(Stream &os) 
	{
		Internal::OutputUnit2<U>::fn(os);
	}
};


UNIT_DISPLAY_NAME(Unit, "1");


namespace Internal
{
	template <typename U1, typename U2>
	struct OutputUnit2< Compose<U1,U2> >
	{
		template <typename Stream>
		static void fn(Stream &os) 
		{ 
			OutputUnit<U1>::fn(os); 
			os << '.';
			OutputUnit<U2>::fn(os); 
		}
	};

	template <typename U, int Num, int Den>
	struct OutputUnit2< Power<U, Num, Den > >
	{
		template <typename Stream>
		static void fn(Stream &os) 
		{ 
			if(Num!=Den) os << '(';
			OutputUnit<U>::fn(os); 
			if(Num!=Den)
			{
				os << ')';
				os << '^' << Num;
				if(Num%Den)
				{
					os << '/' << Den;
				}
			}
		}
	};

	template <typename U, int Num, int Den>
	struct OutputUnit2< Translate<U, Num, Den > >
	{
		template <typename Stream>
		static void fn(Stream &os) 
		{ 
			os << '(';
			OutputUnit<U>::fn(os); 
			os << '+' << Num;
			if(Den!=1) os << '/' << Den;
			os << ')';
		}
	};

	template <typename U, int Num, int Den>
	struct OutputUnit2< Scale<U, Num, Den > >
	{
		template <typename Stream>
		static void fn(Stream &os) 
		{ 
			os << Den;
			if(Num != 1)
				os << '/' << Num;
			os << '.';
			OutputUnit<U>::fn(os); 
		}
	};
} // namespace Internal


template <typename Str, typename V, typename U>
Str& operator << (Str& os, const Value<V, U>& value)
{
	os << value.get() << ' ';
	OutputUnit<U>::fn(os);
	return os;
}


///
/// Additional Units
///


namespace Units
{
	typedef Poco::Util::Units::Unit Unit;

	// SI base Units:

	struct m;	/// meter
	struct kg;	/// kilogram
	struct s;	/// second
	struct K;	/// Kelvin
	struct A;	/// Ampere
	struct mol;	/// mole
	struct cd;	/// candela
}


UNIT_DISPLAY_NAME(Units::m,   "m");
UNIT_DISPLAY_NAME(Units::kg,  "kg");
UNIT_DISPLAY_NAME(Units::s,   "s");
UNIT_DISPLAY_NAME(Units::K,   "K");
UNIT_DISPLAY_NAME(Units::A,   "A");
UNIT_DISPLAY_NAME(Units::mol, "mol");
UNIT_DISPLAY_NAME(Units::cd,  "cd");


namespace Units
{
	// SI derived Units:
	typedef Compose<m, Power<m, -1> > rad;
	typedef Compose<Power<m, 2>, Power<m, -2> > sr;
	typedef Power<s, -1> Hz;
	typedef Compose<m, Compose<kg, Power<s, -2> > > N;
	typedef Compose<N, Power<m, -2> > Pa;
	typedef Compose<N, m> J;
	typedef Compose<J, Power<s, -1> > W;
	typedef Compose<s, A> C;
	typedef Compose<W, Power<A, -1> > V;
	typedef Compose<C, Power<V, -1> > F;
	typedef Compose<V, Power<A, -1> > Ohm;
	typedef Compose<A, Power<V, -1> > S;
	typedef Compose<V, s> Wb;
	typedef Compose<Wb, Power<m, -2> > T;
	typedef Compose<Wb, Power<A, -1> > H;
	typedef cd lm;
	typedef Compose<lm, Power<m, -2> > lx;
	typedef Power<s, -1> Bq;
	typedef Compose<J, Power<kg, -1> > Gy;
	typedef Gy Sv;
	typedef Compose<Power<s, -1>,mol> kat;
}


UNIT_DISPLAY_NAME(Units::rad, "rad");
UNIT_DISPLAY_NAME(Units::sr,  "sr");
// UNIT_DISPLAY_NAME(Units::Hz, "Hz");	// Too problematic
UNIT_DISPLAY_NAME(Units::N,   "N");
UNIT_DISPLAY_NAME(Units::Pa,  "Pa");
UNIT_DISPLAY_NAME(Units::J,   "J");
UNIT_DISPLAY_NAME(Units::W,   "W");
UNIT_DISPLAY_NAME(Units::C,   "C");
UNIT_DISPLAY_NAME(Units::V,   "V");
UNIT_DISPLAY_NAME(Units::F,   "F");
UNIT_DISPLAY_NAME(Units::Ohm, "Ohm");
UNIT_DISPLAY_NAME(Units::S,   "S");
UNIT_DISPLAY_NAME(Units::Wb,  "Wb");
UNIT_DISPLAY_NAME(Units::T,   "T");
UNIT_DISPLAY_NAME(Units::H,   "H");
UNIT_DISPLAY_NAME(Units::lx,  "lx");
UNIT_DISPLAY_NAME(Units::Gy,  "Gy");
UNIT_DISPLAY_NAME(Units::kat,  "kat");


namespace Units
{
	// SI prefixes:
	template <typename U> struct deca { typedef Scale<U, 1, 10> type; };
	template <typename U> struct hecto { typedef Scale<U, 1, 100> type; };
	template <typename U> struct kilo { typedef Scale<U, 1, 1000> type; };
	template <typename U> struct mega { typedef Scale<typename kilo<U>::type, 1, 1000> type; };
	template <typename U> struct giga { typedef Scale<typename mega<U>::type, 1, 1000> type; };
	template <typename U> struct tera { typedef Scale<typename giga<U>::type, 1, 1000> type; };
	template <typename U> struct peta { typedef Scale<typename tera<U>::type, 1, 1000> type; };
	template <typename U> struct exa { typedef Scale<typename peta<U>::type, 1, 1000> type; };
	template <typename U> struct zetta { typedef Scale<typename exa<U>::type, 1, 1000> type; };
	template <typename U> struct yotta { typedef Scale<typename zetta<U>::type, 1, 1000> type; };

	template <typename U> struct deci { typedef Scale<U, 10> type; };
	template <typename U> struct centi { typedef Scale<U, 100> type; };
	template <typename U> struct milli { typedef Scale<U, 1000> type; };
	template <typename U> struct micro { typedef Scale<typename milli<U>::type, 1000> type; };
	template <typename U> struct nano { typedef Scale<typename micro<U>::type, 1000> type; };
	template <typename U> struct pico { typedef Scale<typename nano<U>::type, 1000> type; };
	template <typename U> struct femto { typedef Scale<typename pico<U>::type, 1000> type; };
	template <typename U> struct atto { typedef Scale<typename femto<U>::type, 1000> type; };
	template <typename U> struct zepto { typedef Scale<typename atto<U>::type, 1000> type; };
	template <typename U> struct yocto { typedef Scale<typename zepto<U>::type, 1000> type; };


	// Some prefixed SI Units:
	typedef centi<m>::type cm;
	typedef milli<m>::type mm;
	typedef kilo<m>::type km;
	typedef milli<kg>::type g;
	typedef milli<g>::type mg;
	typedef milli<s>::type ms;


	class Prefix
		/// Parent class for unit prefixes.
		/// Use classes inheriting from this class to scale
		/// the values.
	{
	public:
		template <typename T>
		Prefix(const T& val, double multiplier = 1, const std::string& prefix = ""): 
			_pHolder(new Holder<T>(val)),
			_multiplier(multiplier),
			_prefix(prefix)
		{ 
		}

		double value() const
		{ 
			return _pHolder->get() * _multiplier;
		}

		void addPrefix(std::ostream& os) const
		{
			os << _prefix;
		}

		void addUnit(std::ostream& os) const
		{
			_pHolder->appendUnit(os);
		}

	private:
		Prefix();

		class Placeholder
		{
		public:
			virtual ~Placeholder() { }
			virtual double get() const = 0;
			virtual void appendUnit(std::ostream& os) const = 0;
		};

		template <typename U>
		struct Holder : public Placeholder
		{
			typedef Value<typename U::ValueType, typename U::Unit> ValueType;

			Holder (const U& val): _val(ValueType(val)) 
			{
			}

			double get() const
			{
				return _val.get();
			}

			void appendUnit(std::ostream& os) const
			{
				OutputUnit<typename U::Unit>::fn(os);
			}

			ValueType _val;
		};

		Placeholder* _pHolder;
		double       _multiplier;
		std::string  _prefix;
	};
}


template <typename Str>
Str& streamOp (Str& os, const Units::Prefix& val)
{
	os << val.value() << ' ';
	val.addPrefix(os);
	val.addUnit(os);
	return os;
}


template <typename Str>
Str& operator << (Str& os, const Units::Prefix& val)
	/// Streaming operator for prefixed values.
{
	return streamOp(os, val);
}


UNIT_DISPLAY_NAME(Units::cm, "cm");
UNIT_DISPLAY_NAME(Units::mm, "mm");
UNIT_DISPLAY_NAME(Units::km, "km");
UNIT_DISPLAY_NAME(Units::g,  "g");
UNIT_DISPLAY_NAME(Units::mg, "mg");
UNIT_DISPLAY_NAME(Units::ms, "ms");


namespace Units
{
	// Non-SI mass
	typedef Scale<kg, 22046223, 10000000> lb;
	typedef Scale<lb, 16> oz;
	typedef Scale<kg, 1, 1000> tonne;

	// Non-SI temperature
	typedef Translate<K, -27315, 100> Celsius;
	typedef Translate<Scale<Celsius, 9, 5>, 32> Fahrenheit;

	// Non-SI time
	typedef Scale<s, 1, 60> minute;
	typedef Scale<minute, 1, 60> hour;
	typedef Scale<hour, 1, 24> day;
	typedef Scale<day, 1, 7> week;
	struct month;	// No fixed ratio with week 
	typedef Scale<month, 1, 12> year;
	typedef Scale<year, 1, 100> century;
	typedef Scale<year, 1, 1000> millennium;

	// Non-SI length
	typedef Scale<cm, 100, 254> inch;
	typedef Scale<inch, 1, 12> foot;
	typedef Scale<inch, 1, 36> yard;
	typedef Scale<yard, 1, 1760> mile;
	typedef Scale<m, 1, 1852> nautical_mile;

	// Non-SI area
	typedef Power<m, 2> m2;
	typedef Power<mm, 2> mm2;
	typedef Scale<m2, 1, 10000> hectare;
	typedef Scale<m2, 1, 100> are;
	typedef Power<inch, 2> inch2;
	typedef Scale<hectare, 24710538, 10000000> acre;

	// Non-SI volume
	typedef Power<cm, 3> cm3;
	typedef cm3 ml;
	typedef Scale<ml, 1, 1000> liter;
	typedef Scale<liter, 10> dl;
	typedef Scale<liter, 100> cl;
	typedef Power<m, 3> m3;

	// Non-SI velocity
	typedef Compose<mile, Power<hour, -1> > mph;
	typedef Compose<km, Power<hour, -1> > kph;
	typedef Compose<m, Power<s, -1> > meters_per_second;
	typedef Compose<nautical_mile, Power<hour, -1> > knot;
	typedef Scale<meters_per_second, 100, 34029> mach;

	// Angles
	typedef Scale<rad, 180000000, 3141593> degree;
	typedef Scale<rad, 200000000, 3141593> grad;
	typedef Scale< degree, 60 > degree_minute;
	typedef Scale< degree_minute, 60 > degree_second;

	// Pressure
	typedef Scale<Pa, 1, 1000> kPa;
	typedef Scale<kPa, 1450377, 10000000> psi;
	typedef Scale<kPa, 10> millibar;

	// Other
	typedef Scale<Hz, 60> rpm;
	typedef Scale<Unit, 100> percent;
	typedef Scale<Unit, 1, 12> dozen;
	typedef Scale<Unit, 1, 13> bakers_dozen;
}


UNIT_DISPLAY_NAME(Units::lb,            "lb");
UNIT_DISPLAY_NAME(Units::oz,            "oz");
UNIT_DISPLAY_NAME(Units::tonne,         "tonnes");
UNIT_DISPLAY_NAME(Units::Celsius,       "'C");
UNIT_DISPLAY_NAME(Units::Fahrenheit,    "'F");
UNIT_DISPLAY_NAME(Units::minute,        "minutes");
UNIT_DISPLAY_NAME(Units::hour,          "hours");
UNIT_DISPLAY_NAME(Units::day,           "days");
UNIT_DISPLAY_NAME(Units::week,          "weeks");
UNIT_DISPLAY_NAME(Units::month,         "months");
UNIT_DISPLAY_NAME(Units::year,          "years");
UNIT_DISPLAY_NAME(Units::century,       "centuries");
UNIT_DISPLAY_NAME(Units::millennium,    "millennia");
UNIT_DISPLAY_NAME(Units::inch,          "inches");
UNIT_DISPLAY_NAME(Units::foot,          "foot");
UNIT_DISPLAY_NAME(Units::yard,          "yards");
UNIT_DISPLAY_NAME(Units::mile,          "miles");
UNIT_DISPLAY_NAME(Units::nautical_mile, "nautical miles");
UNIT_DISPLAY_NAME(Units::hectare,       "ha");
UNIT_DISPLAY_NAME(Units::are,           "are");
UNIT_DISPLAY_NAME(Units::acre,          "acres");
UNIT_DISPLAY_NAME(Units::ml,            "ml");
UNIT_DISPLAY_NAME(Units::liter,         "l");
UNIT_DISPLAY_NAME(Units::dl,            "dl");
UNIT_DISPLAY_NAME(Units::cl,            "cl");
UNIT_DISPLAY_NAME(Units::mph,           "mph");
UNIT_DISPLAY_NAME(Units::kph,           "km/h");
UNIT_DISPLAY_NAME(Units::knot,          "knots");
UNIT_DISPLAY_NAME(Units::mach,          "mach");
UNIT_DISPLAY_NAME(Units::degree,        "deg");
UNIT_DISPLAY_NAME(Units::grad,          "grad");
UNIT_DISPLAY_NAME(Units::degree_minute, "'");
UNIT_DISPLAY_NAME(Units::degree_second, "\"");
UNIT_DISPLAY_NAME(Units::kPa,           "kPa");
UNIT_DISPLAY_NAME(Units::psi,           "PSI");
UNIT_DISPLAY_NAME(Units::millibar,      "millibars");
UNIT_DISPLAY_NAME(Units::percent,       "%");
UNIT_DISPLAY_NAME(Units::rpm,           "rpm");
UNIT_DISPLAY_NAME(Units::dozen,         "dozen");
UNIT_DISPLAY_NAME(Units::bakers_dozen,  "bakers dozen");


namespace Values
{
	typedef Value<double, Units::Unit> Unit;

	// SI Units
	typedef Value<double, Units::m> m;
	typedef Value<double, Units::kg> kg;
	typedef Value<double, Units::s> s;
	typedef Value<double, Units::K> K;
	typedef Value<double, Units::A> A;
	typedef Value<double, Units::mol> mol;
	typedef Value<double, Units::cd> cd;

	// SI derived
	typedef Value<double, Units::rad> rad;
	typedef Value<double, Units::sr> sr;
	typedef Value<double, Units::Hz> Hz;
	typedef Value<double, Units::N> N;
	typedef Value<double, Units::Pa> Pa;
	typedef Value<double, Units::J> J;
	typedef Value<double, Units::W> W;
	typedef Value<double, Units::C> C;
	typedef Value<double, Units::V> V;
	typedef Value<double, Units::F> F;
	typedef Value<double, Units::Ohm> Ohm;
	typedef Value<double, Units::S> S;
	typedef Value<double, Units::Wb> Wb;
	typedef Value<double, Units::T> T;
	typedef Value<double, Units::H> H;
	typedef Value<double, Units::lm> lm;
	typedef Value<double, Units::lx> lx;
	typedef Value<double, Units::Bq> Bq;
	typedef Value<double, Units::Gy> Gy;
	typedef Value<double, Units::Sv> Sv;
	typedef Value<double, Units::kat> kat;

	// Prefixed Units
	typedef Value<double, Units::cm> cm;
	typedef Value<double, Units::mm> mm;
	typedef Value<double, Units::km> km;
	typedef Value<double, Units::g> g;
	typedef Value<double, Units::mg> mg;
	typedef Value<double, Units::ms> ms;

	// Non-SI
	typedef Value<double, Units::lb> lb;
	typedef Value<double, Units::oz> oz;
	typedef Value<double, Units::tonne> tonne;

	typedef Value<double, Units::Celsius> Celsius;
	typedef Value<double, Units::Fahrenheit> Fahrenheit;

	typedef Value<double, Units::minute> minute;
	typedef Value<double, Units::hour> hour;
	typedef Value<double, Units::day> day;
	typedef Value<double, Units::week> week;
	typedef Value<double, Units::month> month;
	typedef Value<double, Units::year> year;
	typedef Value<double, Units::century> century;
	typedef Value<double, Units::millennium> millennium;

	typedef Value<double, Units::inch> inch;
	typedef Value<double, Units::foot> foot;
	typedef Value<double, Units::yard> yard;
	typedef Value<double, Units::mile> mile;
	typedef Value<double, Units::nautical_mile> nautical_mile;

	typedef Value<double, Units::m2> m2;
	typedef Value<double, Units::mm2> mm2;
	typedef Value<double, Units::hectare> hectare;
	typedef Value<double, Units::are> are;
	typedef Value<double, Units::inch2> inch2;
	typedef Value<double, Units::acre> acre;

	typedef Value<double, Units::cm3> cm3;
	typedef Value<double, Units::ml> ml;
	typedef Value<double, Units::cl> cl;
	typedef Value<double, Units::liter> liter;
	typedef Value<double, Units::dl> dl;
	typedef Value<double, Units::m3> m3;

	typedef Value<double, Units::mph> mph;
	typedef Value<double, Units::kph> kph;
	typedef Value<double, Units::meters_per_second> meters_per_second;
	typedef Value<double, Units::knot> knot;
	typedef Value<double, Units::mach> mach;

	typedef Value<double, Units::degree> degree;
	typedef Value<double, Units::grad> grad;
	typedef Value<double, Units::degree_minute> degree_minute;
	typedef Value<double, Units::degree_second> degree_second;

	typedef Value<double, Units::kPa> kPa;
	typedef Value<double, Units::psi> psi;
	typedef Value<double, Units::millibar> millibar;

	typedef Value<double, Units::percent> percent;
	typedef Value<double, Units::rpm> rpm;
	typedef Value<double, Units::dozen> dozen;
	typedef Value<double, Units::bakers_dozen> bakers_dozen;

	#define DEFINE_PREFIX_CLASS(name, scale, prefix) \
		struct name: public Units::Prefix \
		{ \
			template <typename T> \
			name(const T& val): Prefix(val, scale, prefix) \
			{ \
			} \
		}; \
		template <typename Str> \
		Str& operator << (Str& os, const name& val) \
		{ \
			return streamOp<Str>(os, val); \
		}

	DEFINE_PREFIX_CLASS (deca, .1, "da")
	DEFINE_PREFIX_CLASS (hecto, .01, "h")
	DEFINE_PREFIX_CLASS (kilo, .001, "k")
	DEFINE_PREFIX_CLASS (mega, 1e-6, "M")
	DEFINE_PREFIX_CLASS (giga, 1e-9, "G")
	DEFINE_PREFIX_CLASS (tera, 1e-12, "T") 
	DEFINE_PREFIX_CLASS (peta, 1e-15, "P")
	DEFINE_PREFIX_CLASS (exa, 1e-18, "E")
	DEFINE_PREFIX_CLASS (zetta, 1e-21, "Z")
	DEFINE_PREFIX_CLASS (yotta, 1e-24, "Y")

	DEFINE_PREFIX_CLASS (deci, 10, "d")
	DEFINE_PREFIX_CLASS (centi, 100, "c")
	DEFINE_PREFIX_CLASS (milli, 1000, "m")
	DEFINE_PREFIX_CLASS (micro, 1e6, "u")
	DEFINE_PREFIX_CLASS (nano, 1e9, "n")
	DEFINE_PREFIX_CLASS (pico, 1e12, "p")
	DEFINE_PREFIX_CLASS (femto, 1e15, "f")
	DEFINE_PREFIX_CLASS (atto, 1e18, "a")
	DEFINE_PREFIX_CLASS (zepto, 1e21, "z")
	DEFINE_PREFIX_CLASS (yocto, 1e24, "y")
}


namespace Constants
{
	// Physical constants:
	const Value<double, Compose<Units::J, Power<Units::K, -1> > > k (1.3806504e-23);
	const Value<double, Units::kg> mu (1.660538782e-27);
	const Value<double, Power<Units::mol, -1> > NA (6.02214179e23);
	const Value<double, Units::s> G0 (7.7480917004e-5);
	const Value<double, Compose<Units::F, Power<Units::m, -1> > > e0 (8.854187817e-12);
	const Value<double, Units::kg> me (9.10938215e-31);
	const Value<double, Units::J> eV (1.602176487e-19);
	const Value<double, Units::C> e (1.602176487e-19);
	const Value<double, Units::F> F (96485.3399);
	const Value<double, Units::Unit> alpha (7.2973525376e-3);
	const Value<double, Units::Unit> inv_alpha (137.035999679);
	const Value<double, Compose<Units::N, Power<Units::A, -2> > > u0 (12.566370614);
	const Value<double, Units::Wb> phi0 (2.067833667e-15);	// ??
	const Value<double, Compose<Units::J, Compose<Power<Units::mol, -1>, Power<Units::kg, -1> > > > R (8.314472);
	const Value<double, Compose< Power<Units::m, 3>, Compose<Power<Units::kg, -1>, Power<Units::s, -2> > > > G (6.67428e-11);
	const Value<double, Compose< Units::J, Units::s > > h (6.62606896e-34);
	const Value<double, Compose< Units::J, Units::s > > h_bar (1.054571628e-34);
	const Value<double, Units::kg> mp (1.672621637e-27);
	const Value<double, Unit> mpme (1836.15267247);
	const Value<double, Power<Units::m, -1> > Rinf (10973731.568527);
	const Value<double, Compose<Units::m, Power<Units::s, -1> > > c (299792458);
	const Value<double, Compose<Units::W, Compose< Power<Units::m, -1>, Power<Units::K, -4> > > > rho (5.6704e-8);

	// Other constants:
	const Value<double, Units::rad> pi (3.141592653589793);
	const Value<double, Units::m> lightyear (9.4605284e15);
	const Value<double, Units::km> AU(149597871);
	const Value<double, Compose<Units::m, Power<Units::s, -2> > > g (9.80665);
}


//
// Trigonometry
//


template <typename V, typename U>
V sin(const Value<V, U>& angle)
{
	return std::sin(Value<V, Units::rad>(angle).get());
}


template <typename V, typename U>
V cos(const Value<V, U>& angle)
{
	return std::cos(Value<V, Units::rad>(angle).get());
}


template <typename V, typename U>
V tan(const Value<V, U>& angle)
{
	return std::tan(Value<V, Units::rad>(angle).get());
}


} } } // namespace Poco::Util::Units


#endif // Util_Units_INCLUDED
