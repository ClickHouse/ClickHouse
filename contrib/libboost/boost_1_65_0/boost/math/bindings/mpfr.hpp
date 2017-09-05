//  Copyright John Maddock 2008.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Wrapper that works with mpfr_class defined in gmpfrxx.h
// See http://math.berkeley.edu/~wilken/code/gmpfrxx/
// Also requires the gmp and mpfr libraries.
//

#ifndef BOOST_MATH_MPLFR_BINDINGS_HPP
#define BOOST_MATH_MPLFR_BINDINGS_HPP

#include <boost/config.hpp>
#include <boost/lexical_cast.hpp>

#ifdef BOOST_MSVC
//
// We get a lot of warnings from the gmp, mpfr and gmpfrxx headers, 
// disable them here, so we only see warnings from *our* code:
//
#pragma warning(push)
#pragma warning(disable: 4127 4800 4512)
#endif

#include <gmpfrxx.h>

#ifdef BOOST_MSVC
#pragma warning(pop)
#endif

#include <boost/math/tools/precision.hpp>
#include <boost/math/tools/real_cast.hpp>
#include <boost/math/policies/policy.hpp>
#include <boost/math/distributions/fwd.hpp>
#include <boost/math/special_functions/math_fwd.hpp>
#include <boost/math/bindings/detail/big_digamma.hpp>
#include <boost/math/bindings/detail/big_lanczos.hpp>
#include <boost/math/tools/big_constant.hpp>

inline mpfr_class fabs(const mpfr_class& v)
{
   return abs(v);
}
template <class T, class U>
inline mpfr_class fabs(const __gmp_expr<T,U>& v)
{
   return abs(static_cast<mpfr_class>(v));
}

inline mpfr_class pow(const mpfr_class& b, const mpfr_class& e)
{
   mpfr_class result;
   mpfr_pow(result.__get_mp(), b.__get_mp(), e.__get_mp(), GMP_RNDN);
   return result;
}
/*
template <class T, class U, class V, class W>
inline mpfr_class pow(const __gmp_expr<T,U>& b, const __gmp_expr<V,W>& e)
{
   return pow(static_cast<mpfr_class>(b), static_cast<mpfr_class>(e));
}
*/
inline mpfr_class ldexp(const mpfr_class& v, int e)
{
   //int e = mpfr_get_exp(*v.__get_mp());
   mpfr_class result(v);
   mpfr_set_exp(result.__get_mp(), e);
   return result;
}
template <class T, class U>
inline mpfr_class ldexp(const __gmp_expr<T,U>& v, int e)
{
   return ldexp(static_cast<mpfr_class>(v), e);
}

inline mpfr_class frexp(const mpfr_class& v, int* expon)
{
   int e = mpfr_get_exp(v.__get_mp());
   mpfr_class result(v);
   mpfr_set_exp(result.__get_mp(), 0);
   *expon = e;
   return result;
}
template <class T, class U>
inline mpfr_class frexp(const __gmp_expr<T,U>& v, int* expon)
{
   return frexp(static_cast<mpfr_class>(v), expon);
}

inline mpfr_class fmod(const mpfr_class& v1, const mpfr_class& v2)
{
   mpfr_class n;
   if(v1 < 0)
      n = ceil(v1 / v2);
   else
      n = floor(v1 / v2);
   return v1 - n * v2;
}
template <class T, class U, class V, class W>
inline mpfr_class fmod(const __gmp_expr<T,U>& v1, const __gmp_expr<V,W>& v2)
{
   return fmod(static_cast<mpfr_class>(v1), static_cast<mpfr_class>(v2));
}

template <class Policy>
inline mpfr_class modf(const mpfr_class& v, long long* ipart, const Policy& pol)
{
   *ipart = lltrunc(v, pol);
   return v - boost::math::tools::real_cast<mpfr_class>(*ipart);
}
template <class T, class U, class Policy>
inline mpfr_class modf(const __gmp_expr<T,U>& v, long long* ipart, const Policy& pol)
{
   return modf(static_cast<mpfr_class>(v), ipart, pol);
}

template <class Policy>
inline int iround(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<int>(boost::math::round(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline int iround(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return iround(static_cast<mpfr_class>(x), pol);
}

template <class Policy>
inline long lround(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<long>(boost::math::round(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline long lround(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return lround(static_cast<mpfr_class>(x), pol);
}

template <class Policy>
inline long long llround(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<long long>(boost::math::round(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline long long llround(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return llround(static_cast<mpfr_class>(x), pol);
}

template <class Policy>
inline int itrunc(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<int>(boost::math::trunc(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline int itrunc(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return itrunc(static_cast<mpfr_class>(x), pol);
}

template <class Policy>
inline long ltrunc(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<long>(boost::math::trunc(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline long ltrunc(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return ltrunc(static_cast<mpfr_class>(x), pol);
}

template <class Policy>
inline long long lltrunc(mpfr_class const& x, const Policy&)
{
   return boost::math::tools::real_cast<long long>(boost::math::trunc(x, typename boost::math::policies::normalise<Policy, boost::math::policies::rounding_error< boost::math::policies::throw_on_error> >::type()));
}
template <class T, class U, class Policy>
inline long long lltrunc(__gmp_expr<T,U> const& x, const Policy& pol)
{
   return lltrunc(static_cast<mpfr_class>(x), pol);
}

namespace boost{ 

#ifdef BOOST_MATH_USE_FLOAT128
   template<> struct is_convertible<BOOST_MATH_FLOAT128_TYPE, mpfr_class> : public boost::integral_constant<bool, false>{};
#endif
   template<> struct is_convertible<long long, mpfr_class> : public boost::integral_constant<bool, false>{};

namespace math{

#if defined(__GNUC__) && (__GNUC__ < 4)
   using ::iround;
   using ::lround;
   using ::llround;
   using ::itrunc;
   using ::ltrunc;
   using ::lltrunc;
   using ::modf;
#endif

namespace lanczos{

struct mpfr_lanczos
{
   static mpfr_class lanczos_sum(const mpfr_class& z)
   {
      unsigned long p = z.get_dprec();
      if(p <= 72)
         return lanczos13UDT::lanczos_sum(z);
      else if(p <= 120)
         return lanczos22UDT::lanczos_sum(z);
      else if(p <= 170)
         return lanczos31UDT::lanczos_sum(z);
      else //if(p <= 370) approx 100 digit precision:
         return lanczos61UDT::lanczos_sum(z);
   }
   static mpfr_class lanczos_sum_expG_scaled(const mpfr_class& z)
   {
      unsigned long p = z.get_dprec();
      if(p <= 72)
         return lanczos13UDT::lanczos_sum_expG_scaled(z);
      else if(p <= 120)
         return lanczos22UDT::lanczos_sum_expG_scaled(z);
      else if(p <= 170)
         return lanczos31UDT::lanczos_sum_expG_scaled(z);
      else //if(p <= 370) approx 100 digit precision:
         return lanczos61UDT::lanczos_sum_expG_scaled(z);
   }
   static mpfr_class lanczos_sum_near_1(const mpfr_class& z)
   {
      unsigned long p = z.get_dprec();
      if(p <= 72)
         return lanczos13UDT::lanczos_sum_near_1(z);
      else if(p <= 120)
         return lanczos22UDT::lanczos_sum_near_1(z);
      else if(p <= 170)
         return lanczos31UDT::lanczos_sum_near_1(z);
      else //if(p <= 370) approx 100 digit precision:
         return lanczos61UDT::lanczos_sum_near_1(z);
   }
   static mpfr_class lanczos_sum_near_2(const mpfr_class& z)
   {
      unsigned long p = z.get_dprec();
      if(p <= 72)
         return lanczos13UDT::lanczos_sum_near_2(z);
      else if(p <= 120)
         return lanczos22UDT::lanczos_sum_near_2(z);
      else if(p <= 170)
         return lanczos31UDT::lanczos_sum_near_2(z);
      else //if(p <= 370) approx 100 digit precision:
         return lanczos61UDT::lanczos_sum_near_2(z);
   }
   static mpfr_class g()
   { 
      unsigned long p = mpfr_class::get_dprec();
      if(p <= 72)
         return lanczos13UDT::g();
      else if(p <= 120)
         return lanczos22UDT::g();
      else if(p <= 170)
         return lanczos31UDT::g();
      else //if(p <= 370) approx 100 digit precision:
         return lanczos61UDT::g();
   }
};

template<class Policy>
struct lanczos<mpfr_class, Policy>
{
   typedef mpfr_lanczos type;
};

} // namespace lanczos

namespace constants{

template <class Real, class Policy>
struct construction_traits;

template <class Policy>
struct construction_traits<mpfr_class, Policy>
{
   typedef mpl::int_<0> type;
};

}

namespace tools
{

template <class T, class U>
struct promote_arg<__gmp_expr<T,U> >
{ // If T is integral type, then promote to double.
  typedef mpfr_class type;
};

template<>
inline int digits<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class)) BOOST_NOEXCEPT
{
   return mpfr_class::get_dprec();
}

namespace detail{

template<class I>
void convert_to_long_result(mpfr_class const& r, I& result)
{
   result = 0;
   I last_result(0);
   mpfr_class t(r);
   double term;
   do
   {
      term = real_cast<double>(t);
      last_result = result;
      result += static_cast<I>(term);
      t -= term;
   }while(result != last_result);
}

}

template <>
inline mpfr_class real_cast<mpfr_class, long long>(long long t)
{
   mpfr_class result;
   int expon = 0;
   int sign = 1;
   if(t < 0)
   {
      sign = -1;
      t = -t;
   }
   while(t)
   {
      result += ldexp((double)(t & 0xffffL), expon);
      expon += 32;
      t >>= 32;
   }
   return result * sign;
}
template <>
inline unsigned real_cast<unsigned, mpfr_class>(mpfr_class t)
{
   return t.get_ui();
}
template <>
inline int real_cast<int, mpfr_class>(mpfr_class t)
{
   return t.get_si();
}
template <>
inline double real_cast<double, mpfr_class>(mpfr_class t)
{
   return t.get_d();
}
template <>
inline float real_cast<float, mpfr_class>(mpfr_class t)
{
   return static_cast<float>(t.get_d());
}
template <>
inline long real_cast<long, mpfr_class>(mpfr_class t)
{
   long result;
   detail::convert_to_long_result(t, result);
   return result;
}
template <>
inline long long real_cast<long long, mpfr_class>(mpfr_class t)
{
   long long result;
   detail::convert_to_long_result(t, result);
   return result;
}

template <>
inline mpfr_class max_value<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class))
{
   static bool has_init = false;
   static mpfr_class val;
   if(!has_init)
   {
      val = 0.5;
      mpfr_set_exp(val.__get_mp(), mpfr_get_emax());
      has_init = true;
   }
   return val;
}

template <>
inline mpfr_class min_value<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class))
{
   static bool has_init = false;
   static mpfr_class val;
   if(!has_init)
   {
      val = 0.5;
      mpfr_set_exp(val.__get_mp(), mpfr_get_emin());
      has_init = true;
   }
   return val;
}

template <>
inline mpfr_class log_max_value<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class))
{
   static bool has_init = false;
   static mpfr_class val = max_value<mpfr_class>();
   if(!has_init)
   {
      val = log(val);
      has_init = true;
   }
   return val;
}

template <>
inline mpfr_class log_min_value<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class))
{
   static bool has_init = false;
   static mpfr_class val = max_value<mpfr_class>();
   if(!has_init)
   {
      val = log(val);
      has_init = true;
   }
   return val;
}

template <>
inline mpfr_class epsilon<mpfr_class>(BOOST_MATH_EXPLICIT_TEMPLATE_TYPE_SPEC(mpfr_class))
{
   return ldexp(mpfr_class(1), 1-boost::math::policies::digits<mpfr_class, boost::math::policies::policy<> >());
}

} // namespace tools

namespace policies{

template <class T, class U, class Policy>
struct evaluation<__gmp_expr<T, U>, Policy>
{
   typedef mpfr_class type;
};

}

template <class Policy>
inline mpfr_class skewness(const extreme_value_distribution<mpfr_class, Policy>& /*dist*/)
{
   //
   // This is 12 * sqrt(6) * zeta(3) / pi^3:
   // See http://mathworld.wolfram.com/ExtremeValueDistribution.html
   //
   return boost::lexical_cast<mpfr_class>("1.1395470994046486574927930193898461120875997958366");
}

template <class Policy>
inline mpfr_class skewness(const rayleigh_distribution<mpfr_class, Policy>& /*dist*/)
{
  // using namespace boost::math::constants;
  return boost::lexical_cast<mpfr_class>("0.63111065781893713819189935154422777984404221106391");
  // Computed using NTL at 150 bit, about 50 decimal digits.
  // return 2 * root_pi<RealType>() * pi_minus_three<RealType>() / pow23_four_minus_pi<RealType>();
}

template <class Policy>
inline mpfr_class kurtosis(const rayleigh_distribution<mpfr_class, Policy>& /*dist*/)
{
  // using namespace boost::math::constants;
  return boost::lexical_cast<mpfr_class>("3.2450893006876380628486604106197544154170667057995");
  // Computed using NTL at 150 bit, about 50 decimal digits.
  // return 3 - (6 * pi<RealType>() * pi<RealType>() - 24 * pi<RealType>() + 16) /
  // (four_minus_pi<RealType>() * four_minus_pi<RealType>());
}

template <class Policy>
inline mpfr_class kurtosis_excess(const rayleigh_distribution<mpfr_class, Policy>& /*dist*/)
{
  //using namespace boost::math::constants;
  // Computed using NTL at 150 bit, about 50 decimal digits.
  return boost::lexical_cast<mpfr_class>("0.2450893006876380628486604106197544154170667057995");
  // return -(6 * pi<RealType>() * pi<RealType>() - 24 * pi<RealType>() + 16) /
  //   (four_minus_pi<RealType>() * four_minus_pi<RealType>());
} // kurtosis

namespace detail{

//
// Version of Digamma accurate to ~100 decimal digits.
//
template <class Policy>
mpfr_class digamma_imp(mpfr_class x, const mpl::int_<0>* , const Policy& pol)
{
   //
   // This handles reflection of negative arguments, and all our
   // empfr_classor handling, then forwards to the T-specific approximation.
   //
   BOOST_MATH_STD_USING // ADL of std functions.

   mpfr_class result = 0;
   //
   // Check for negative arguments and use reflection:
   //
   if(x < 0)
   {
      // Reflect:
      x = 1 - x;
      // Argument reduction for tan:
      mpfr_class remainder = x - floor(x);
      // Shift to negative if > 0.5:
      if(remainder > 0.5)
      {
         remainder -= 1;
      }
      //
      // check for evaluation at a negative pole:
      //
      if(remainder == 0)
      {
         return policies::raise_pole_error<mpfr_class>("boost::math::digamma<%1%>(%1%)", 0, (1-x), pol);
      }
      result = constants::pi<mpfr_class>() / tan(constants::pi<mpfr_class>() * remainder);
   }
   result += big_digamma(x);
   return result;
}
//
// Specialisations of this function provides the initial
// starting guess for Halley iteration:
//
template <class Policy>
inline mpfr_class erf_inv_imp(const mpfr_class& p, const mpfr_class& q, const Policy&, const boost::mpl::int_<64>*)
{
   BOOST_MATH_STD_USING // for ADL of std names.

   mpfr_class result = 0;
   
   if(p <= 0.5)
   {
      //
      // Evaluate inverse erf using the rational approximation:
      //
      // x = p(p+10)(Y+R(p))
      //
      // Where Y is a constant, and R(p) is optimised for a low
      // absolute empfr_classor compared to |Y|.
      //
      // double: Max empfr_classor found: 2.001849e-18
      // long double: Max empfr_classor found: 1.017064e-20
      // Maximum Deviation Found (actual empfr_classor term at infinite precision) 8.030e-21
      //
      static const float Y = 0.0891314744949340820313f;
      static const mpfr_class P[] = {    
         -0.000508781949658280665617,
         -0.00836874819741736770379,
         0.0334806625409744615033,
         -0.0126926147662974029034,
         -0.0365637971411762664006,
         0.0219878681111168899165,
         0.00822687874676915743155,
         -0.00538772965071242932965
      };
      static const mpfr_class Q[] = {    
         1,
         -0.970005043303290640362,
         -1.56574558234175846809,
         1.56221558398423026363,
         0.662328840472002992063,
         -0.71228902341542847553,
         -0.0527396382340099713954,
         0.0795283687341571680018,
         -0.00233393759374190016776,
         0.000886216390456424707504
      };
      mpfr_class g = p * (p + 10);
      mpfr_class r = tools::evaluate_polynomial(P, p) / tools::evaluate_polynomial(Q, p);
      result = g * Y + g * r;
   }
   else if(q >= 0.25)
   {
      //
      // Rational approximation for 0.5 > q >= 0.25
      //
      // x = sqrt(-2*log(q)) / (Y + R(q))
      //
      // Where Y is a constant, and R(q) is optimised for a low
      // absolute empfr_classor compared to Y.
      //
      // double : Max empfr_classor found: 7.403372e-17
      // long double : Max empfr_classor found: 6.084616e-20
      // Maximum Deviation Found (empfr_classor term) 4.811e-20
      //
      static const float Y = 2.249481201171875f;
      static const mpfr_class P[] = {    
         -0.202433508355938759655,
         0.105264680699391713268,
         8.37050328343119927838,
         17.6447298408374015486,
         -18.8510648058714251895,
         -44.6382324441786960818,
         17.445385985570866523,
         21.1294655448340526258,
         -3.67192254707729348546
      };
      static const mpfr_class Q[] = {    
         1,
         6.24264124854247537712,
         3.9713437953343869095,
         -28.6608180499800029974,
         -20.1432634680485188801,
         48.5609213108739935468,
         10.8268667355460159008,
         -22.6436933413139721736,
         1.72114765761200282724
      };
      mpfr_class g = sqrt(-2 * log(q));
      mpfr_class xs = q - 0.25;
      mpfr_class r = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
      result = g / (Y + r);
   }
   else
   {
      //
      // For q < 0.25 we have a series of rational approximations all
      // of the general form:
      //
      // let: x = sqrt(-log(q))
      //
      // Then the result is given by:
      //
      // x(Y+R(x-B))
      //
      // where Y is a constant, B is the lowest value of x for which 
      // the approximation is valid, and R(x-B) is optimised for a low
      // absolute empfr_classor compared to Y.
      //
      // Note that almost all code will really go through the first
      // or maybe second approximation.  After than we're dealing with very
      // small input values indeed: 80 and 128 bit long double's go all the
      // way down to ~ 1e-5000 so the "tail" is rather long...
      //
      mpfr_class x = sqrt(-log(q));
      if(x < 3)
      {
         // Max empfr_classor found: 1.089051e-20
         static const float Y = 0.807220458984375f;
         static const mpfr_class P[] = {    
            -0.131102781679951906451,
            -0.163794047193317060787,
            0.117030156341995252019,
            0.387079738972604337464,
            0.337785538912035898924,
            0.142869534408157156766,
            0.0290157910005329060432,
            0.00214558995388805277169,
            -0.679465575181126350155e-6,
            0.285225331782217055858e-7,
            -0.681149956853776992068e-9
         };
         static const mpfr_class Q[] = {    
            1,
            3.46625407242567245975,
            5.38168345707006855425,
            4.77846592945843778382,
            2.59301921623620271374,
            0.848854343457902036425,
            0.152264338295331783612,
            0.01105924229346489121
         };
         mpfr_class xs = x - 1.125;
         mpfr_class R = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
         result = Y * x + R * x;
      }
      else if(x < 6)
      {
         // Max empfr_classor found: 8.389174e-21
         static const float Y = 0.93995571136474609375f;
         static const mpfr_class P[] = {    
            -0.0350353787183177984712,
            -0.00222426529213447927281,
            0.0185573306514231072324,
            0.00950804701325919603619,
            0.00187123492819559223345,
            0.000157544617424960554631,
            0.460469890584317994083e-5,
            -0.230404776911882601748e-9,
            0.266339227425782031962e-11
         };
         static const mpfr_class Q[] = {    
            1,
            1.3653349817554063097,
            0.762059164553623404043,
            0.220091105764131249824,
            0.0341589143670947727934,
            0.00263861676657015992959,
            0.764675292302794483503e-4
         };
         mpfr_class xs = x - 3;
         mpfr_class R = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
         result = Y * x + R * x;
      }
      else if(x < 18)
      {
         // Max empfr_classor found: 1.481312e-19
         static const float Y = 0.98362827301025390625f;
         static const mpfr_class P[] = {    
            -0.0167431005076633737133,
            -0.00112951438745580278863,
            0.00105628862152492910091,
            0.000209386317487588078668,
            0.149624783758342370182e-4,
            0.449696789927706453732e-6,
            0.462596163522878599135e-8,
            -0.281128735628831791805e-13,
            0.99055709973310326855e-16
         };
         static const mpfr_class Q[] = {    
            1,
            0.591429344886417493481,
            0.138151865749083321638,
            0.0160746087093676504695,
            0.000964011807005165528527,
            0.275335474764726041141e-4,
            0.282243172016108031869e-6
         };
         mpfr_class xs = x - 6;
         mpfr_class R = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
         result = Y * x + R * x;
      }
      else if(x < 44)
      {
         // Max empfr_classor found: 5.697761e-20
         static const float Y = 0.99714565277099609375f;
         static const mpfr_class P[] = {    
            -0.0024978212791898131227,
            -0.779190719229053954292e-5,
            0.254723037413027451751e-4,
            0.162397777342510920873e-5,
            0.396341011304801168516e-7,
            0.411632831190944208473e-9,
            0.145596286718675035587e-11,
            -0.116765012397184275695e-17
         };
         static const mpfr_class Q[] = {    
            1,
            0.207123112214422517181,
            0.0169410838120975906478,
            0.000690538265622684595676,
            0.145007359818232637924e-4,
            0.144437756628144157666e-6,
            0.509761276599778486139e-9
         };
         mpfr_class xs = x - 18;
         mpfr_class R = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
         result = Y * x + R * x;
      }
      else
      {
         // Max empfr_classor found: 1.279746e-20
         static const float Y = 0.99941349029541015625f;
         static const mpfr_class P[] = {    
            -0.000539042911019078575891,
            -0.28398759004727721098e-6,
            0.899465114892291446442e-6,
            0.229345859265920864296e-7,
            0.225561444863500149219e-9,
            0.947846627503022684216e-12,
            0.135880130108924861008e-14,
            -0.348890393399948882918e-21
         };
         static const mpfr_class Q[] = {    
            1,
            0.0845746234001899436914,
            0.00282092984726264681981,
            0.468292921940894236786e-4,
            0.399968812193862100054e-6,
            0.161809290887904476097e-8,
            0.231558608310259605225e-11
         };
         mpfr_class xs = x - 44;
         mpfr_class R = tools::evaluate_polynomial(P, xs) / tools::evaluate_polynomial(Q, xs);
         result = Y * x + R * x;
      }
   }
   return result;
}

inline mpfr_class bessel_i0(mpfr_class x)
{
    static const mpfr_class P1[] = {
        boost::lexical_cast<mpfr_class>("-2.2335582639474375249e+15"),
        boost::lexical_cast<mpfr_class>("-5.5050369673018427753e+14"),
        boost::lexical_cast<mpfr_class>("-3.2940087627407749166e+13"),
        boost::lexical_cast<mpfr_class>("-8.4925101247114157499e+11"),
        boost::lexical_cast<mpfr_class>("-1.1912746104985237192e+10"),
        boost::lexical_cast<mpfr_class>("-1.0313066708737980747e+08"),
        boost::lexical_cast<mpfr_class>("-5.9545626019847898221e+05"),
        boost::lexical_cast<mpfr_class>("-2.4125195876041896775e+03"),
        boost::lexical_cast<mpfr_class>("-7.0935347449210549190e+00"),
        boost::lexical_cast<mpfr_class>("-1.5453977791786851041e-02"),
        boost::lexical_cast<mpfr_class>("-2.5172644670688975051e-05"),
        boost::lexical_cast<mpfr_class>("-3.0517226450451067446e-08"),
        boost::lexical_cast<mpfr_class>("-2.6843448573468483278e-11"),
        boost::lexical_cast<mpfr_class>("-1.5982226675653184646e-14"),
        boost::lexical_cast<mpfr_class>("-5.2487866627945699800e-18"),
    };
    static const mpfr_class Q1[] = {
        boost::lexical_cast<mpfr_class>("-2.2335582639474375245e+15"),
        boost::lexical_cast<mpfr_class>("7.8858692566751002988e+12"),
        boost::lexical_cast<mpfr_class>("-1.2207067397808979846e+10"),
        boost::lexical_cast<mpfr_class>("1.0377081058062166144e+07"),
        boost::lexical_cast<mpfr_class>("-4.8527560179962773045e+03"),
        boost::lexical_cast<mpfr_class>("1.0"),
    };
    static const mpfr_class P2[] = {
        boost::lexical_cast<mpfr_class>("-2.2210262233306573296e-04"),
        boost::lexical_cast<mpfr_class>("1.3067392038106924055e-02"),
        boost::lexical_cast<mpfr_class>("-4.4700805721174453923e-01"),
        boost::lexical_cast<mpfr_class>("5.5674518371240761397e+00"),
        boost::lexical_cast<mpfr_class>("-2.3517945679239481621e+01"),
        boost::lexical_cast<mpfr_class>("3.1611322818701131207e+01"),
        boost::lexical_cast<mpfr_class>("-9.6090021968656180000e+00"),
    };
    static const mpfr_class Q2[] = {
        boost::lexical_cast<mpfr_class>("-5.5194330231005480228e-04"),
        boost::lexical_cast<mpfr_class>("3.2547697594819615062e-02"),
        boost::lexical_cast<mpfr_class>("-1.1151759188741312645e+00"),
        boost::lexical_cast<mpfr_class>("1.3982595353892851542e+01"),
        boost::lexical_cast<mpfr_class>("-6.0228002066743340583e+01"),
        boost::lexical_cast<mpfr_class>("8.5539563258012929600e+01"),
        boost::lexical_cast<mpfr_class>("-3.1446690275135491500e+01"),
        boost::lexical_cast<mpfr_class>("1.0"),
    };
    mpfr_class value, factor, r;

    BOOST_MATH_STD_USING
    using namespace boost::math::tools;

    if (x < 0)
    {
        x = -x;                         // even function
    }
    if (x == 0)
    {
        return static_cast<mpfr_class>(1);
    }
    if (x <= 15)                        // x in (0, 15]
    {
        mpfr_class y = x * x;
        value = evaluate_polynomial(P1, y) / evaluate_polynomial(Q1, y);
    }
    else                                // x in (15, \infty)
    {
        mpfr_class y = 1 / x - mpfr_class(1) / 15;
        r = evaluate_polynomial(P2, y) / evaluate_polynomial(Q2, y);
        factor = exp(x) / sqrt(x);
        value = factor * r;
    }

    return value;
}

inline mpfr_class bessel_i1(mpfr_class x)
{
    static const mpfr_class P1[] = {
        static_cast<mpfr_class>("-1.4577180278143463643e+15"),
        static_cast<mpfr_class>("-1.7732037840791591320e+14"),
        static_cast<mpfr_class>("-6.9876779648010090070e+12"),
        static_cast<mpfr_class>("-1.3357437682275493024e+11"),
        static_cast<mpfr_class>("-1.4828267606612366099e+09"),
        static_cast<mpfr_class>("-1.0588550724769347106e+07"),
        static_cast<mpfr_class>("-5.1894091982308017540e+04"),
        static_cast<mpfr_class>("-1.8225946631657315931e+02"),
        static_cast<mpfr_class>("-4.7207090827310162436e-01"),
        static_cast<mpfr_class>("-9.1746443287817501309e-04"),
        static_cast<mpfr_class>("-1.3466829827635152875e-06"),
        static_cast<mpfr_class>("-1.4831904935994647675e-09"),
        static_cast<mpfr_class>("-1.1928788903603238754e-12"),
        static_cast<mpfr_class>("-6.5245515583151902910e-16"),
        static_cast<mpfr_class>("-1.9705291802535139930e-19"),
    };
    static const mpfr_class Q1[] = {
        static_cast<mpfr_class>("-2.9154360556286927285e+15"),
        static_cast<mpfr_class>("9.7887501377547640438e+12"),
        static_cast<mpfr_class>("-1.4386907088588283434e+10"),
        static_cast<mpfr_class>("1.1594225856856884006e+07"),
        static_cast<mpfr_class>("-5.1326864679904189920e+03"),
        static_cast<mpfr_class>("1.0"),
    };
    static const mpfr_class P2[] = {
        static_cast<mpfr_class>("1.4582087408985668208e-05"),
        static_cast<mpfr_class>("-8.9359825138577646443e-04"),
        static_cast<mpfr_class>("2.9204895411257790122e-02"),
        static_cast<mpfr_class>("-3.4198728018058047439e-01"),
        static_cast<mpfr_class>("1.3960118277609544334e+00"),
        static_cast<mpfr_class>("-1.9746376087200685843e+00"),
        static_cast<mpfr_class>("8.5591872901933459000e-01"),
        static_cast<mpfr_class>("-6.0437159056137599999e-02"),
    };
    static const mpfr_class Q2[] = {
        static_cast<mpfr_class>("3.7510433111922824643e-05"),
        static_cast<mpfr_class>("-2.2835624489492512649e-03"),
        static_cast<mpfr_class>("7.4212010813186530069e-02"),
        static_cast<mpfr_class>("-8.5017476463217924408e-01"),
        static_cast<mpfr_class>("3.2593714889036996297e+00"),
        static_cast<mpfr_class>("-3.8806586721556593450e+00"),
        static_cast<mpfr_class>("1.0"),
    };
    mpfr_class value, factor, r, w;

    BOOST_MATH_STD_USING
    using namespace boost::math::tools;

    w = abs(x);
    if (x == 0)
    {
        return static_cast<mpfr_class>(0);
    }
    if (w <= 15)                        // w in (0, 15]
    {
        mpfr_class y = x * x;
        r = evaluate_polynomial(P1, y) / evaluate_polynomial(Q1, y);
        factor = w;
        value = factor * r;
    }
    else                                // w in (15, \infty)
    {
        mpfr_class y = 1 / w - mpfr_class(1) / 15;
        r = evaluate_polynomial(P2, y) / evaluate_polynomial(Q2, y);
        factor = exp(w) / sqrt(w);
        value = factor * r;
    }

    if (x < 0)
    {
        value *= -value;                 // odd function
    }
    return value;
}

} // namespace detail

}

template<> struct is_convertible<long double, mpfr_class> : public mpl::false_{};

}

#endif // BOOST_MATH_MPLFR_BINDINGS_HPP

