///////////////////////////////////////////////////////////////////
//  Copyright Christopher Kormanyos 1999 - 2020.                 //
//  Distributed under the Boost Software License,                //
//  Version 1.0. (See accompanying file LICENSE_1_0.txt          //
//  or copy at http://www.boost.org/LICENSE_1_0.txt)             //
///////////////////////////////////////////////////////////////////

#ifndef GENERIC_TEMPLATE_UINTWIDE_T_2018_10_02_H_
  #define GENERIC_TEMPLATE_UINTWIDE_T_2018_10_02_H_

  #include <algorithm>
  #include <array>
  #include <cstddef>
  #include <cstdint>
  #include <cstring>
  #include <initializer_list>
  #include <iterator>
  #include <limits>
  #include <type_traits>

  #if defined(WIDE_INTEGER_DISABLE_IOSTREAM)
  #else
  #include <iomanip>
  #include <istream>
  #include <ostream>
  #include <sstream>
  #endif

  namespace wide_integer { namespace generic_template {

  // Forward declaration of the uintwide_t template class.
  template<const std::uint_fast32_t Digits2,
           typename LimbType = std::uint32_t>
  class uintwide_t;

  // Forward declarations of non-member binary add, sub, mul, div, mod of (uintwide_t op uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator+(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator-(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator*(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator/(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator%(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);

  // Forward declarations of non-member binary add, sub, mul, div, mod of (uintwide_t op IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator+(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator-(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator*(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator/(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == false)), uintwide_t<Digits2, LimbType>>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == true)
                           && std::numeric_limits<IntegralType>::digits <= (std::numeric_limits<LimbType>::digits)), typename uintwide_t<Digits2, LimbType>::limb_type>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == true)
                           && (std::numeric_limits<IntegralType>::digits > std::numeric_limits<LimbType>::digits)), uintwide_t<Digits2, LimbType>>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  // Forward declarations of non-member binary add, sub, mul, div, mod of (IntegralType op uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator+(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator-(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator*(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator/(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator%(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  // Forward declarations of non-member binary logic operations of (uintwide_t op uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator|(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator^(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator&(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);

  // Forward declarations of non-member binary logic operations of (uintwide_t op IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator|(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator^(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator&(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  // Forward declarations of non-member binary binary logic operations of (IntegralType op uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator|(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator^(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator&(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  // Forward declarations of non-member shift functions of (uintwide_t shift IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator<<(const uintwide_t<Digits2, LimbType>& u, const IntegralType n);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator>>(const uintwide_t<Digits2, LimbType>& u, const IntegralType n);

  // Forward declarations of non-member comparison functions of (uintwide_t cmp uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator==(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator!=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator> (const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator< (const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator>=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator<=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v);

  // Forward declarations of non-member comparison functions of (uintwide_t cmp IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator==(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator!=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator> (const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator< (const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator>=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator<=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v);

  // Forward declarations of non-member comparison functions of (IntegralType cmp uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator==(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator!=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator> (const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator< (const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator>=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator<=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v);

  #if defined(WIDE_INTEGER_DISABLE_IOSTREAM)
  #else

  // Forward declarations of I/O streaming functions.
  template<typename char_type,
           typename traits_type,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  std::basic_ostream<char_type,
                     traits_type>& operator<<(std::basic_ostream<char_type, traits_type>& out,
                                              const uintwide_t<Digits2, LimbType>& x);

  template<typename char_type,
           typename traits_type,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  std::basic_istream<char_type,
                     traits_type>& operator>>(std::basic_istream<char_type, traits_type>& in,
                                              uintwide_t<Digits2, LimbType>& x);

  #endif

  // Forward declarations of various number-theoretical tools.
  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  void swap(uintwide_t<Digits2, LimbType>& x,
            uintwide_t<Digits2, LimbType>& y);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  std::uint_fast32_t lsb(const uintwide_t<Digits2, LimbType>& x);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  std::uint_fast32_t msb(const uintwide_t<Digits2, LimbType>& x);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> sqrt(const uintwide_t<Digits2, LimbType>& m);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> cbrt(const uintwide_t<Digits2, LimbType>& m);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> rootk(const uintwide_t<Digits2, LimbType>& m,
                                      const std::uint_fast8_t k);

  template<typename OtherUnsignedIntegralTypeP,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> pow(const uintwide_t<Digits2, LimbType>& b,
                                    const OtherUnsignedIntegralTypeP&    p);

  template<typename OtherUnsignedIntegralTypeP,
           typename OtherUnsignedIntegralTypeM,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> powm(const uintwide_t<Digits2, LimbType>& b,
                                     const OtherUnsignedIntegralTypeP&    p,
                                     const OtherUnsignedIntegralTypeM&    m);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> gcd(const uintwide_t<Digits2, LimbType>& a,
                                    const uintwide_t<Digits2, LimbType>& b);

  template<typename ST>
  typename std::enable_if<(   (std::is_fundamental<ST>::value == true)
                           && (std::is_integral   <ST>::value == true)
                           && (std::is_unsigned   <ST>::value == true)), ST>::type
  gcd(const ST& u, const ST& v);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  class default_random_engine;

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  class uniform_int_distribution;

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  bool operator==(const uniform_int_distribution<Digits2, LimbType>& lhs,
                  const uniform_int_distribution<Digits2, LimbType>& rhs);

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  bool operator!=(const uniform_int_distribution<Digits2, LimbType>& lhs,
                  const uniform_int_distribution<Digits2, LimbType>& rhs);

  template<typename DistributionType,
           typename GeneratorType,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  bool miller_rabin(const uintwide_t<Digits2, LimbType>& n,
                    const std::uint_fast32_t                    number_of_trials,
                    DistributionType&                    distribution,
                    GeneratorType&                       generator);

  } } // namespace wide_integer::generic_template

  namespace std
  {
    // Forward declaration of specialization of std::numeric_limits<uintwide_t>.
    template<const std::uint_fast32_t Digits2,
             typename LimbType>
    class numeric_limits<wide_integer::generic_template::uintwide_t<Digits2, LimbType>>;
  }

  namespace wide_integer { namespace generic_template { namespace detail {

  template<const std::uint_fast32_t Digits2> struct verify_power_of_two
  {
    static constexpr bool conditional_value =
         (Digits2 == (1ULL <<  0U)) || (Digits2 == (1ULL <<  1U)) || (Digits2 == (1ULL <<  2U)) || (Digits2 == (1ULL <<  3U))
      || (Digits2 == (1ULL <<  4U)) || (Digits2 == (1ULL <<  5U)) || (Digits2 == (1ULL <<  6U)) || (Digits2 == (1ULL <<  7U))
      || (Digits2 == (1ULL <<  8U)) || (Digits2 == (1ULL <<  9U)) || (Digits2 == (1ULL << 10U)) || (Digits2 == (1ULL << 11U))
      || (Digits2 == (1ULL << 12U)) || (Digits2 == (1ULL << 13U)) || (Digits2 == (1ULL << 14U)) || (Digits2 == (1ULL << 15U))
      || (Digits2 == (1ULL << 16U)) || (Digits2 == (1ULL << 17U)) || (Digits2 == (1ULL << 18U)) || (Digits2 == (1ULL << 19U))
      || (Digits2 == (1ULL << 20U)) || (Digits2 == (1ULL << 21U)) || (Digits2 == (1ULL << 22U)) || (Digits2 == (1ULL << 23U))
      || (Digits2 == (1ULL << 24U)) || (Digits2 == (1ULL << 25U)) || (Digits2 == (1ULL << 26U)) || (Digits2 == (1ULL << 27U))
      || (Digits2 == (1ULL << 28U)) || (Digits2 == (1ULL << 29U)) || (Digits2 == (1ULL << 30U)) || (Digits2 == (1ULL << 31U))
      ;
  };

  template<const std::uint_fast32_t Digits2> struct verify_power_of_two_times_granularity_one_sixty_fourth
  {
    // List of numbers used to identify the form 2^n times 1...63.
    static constexpr bool conditional_value =
       (   verify_power_of_two<Digits2 /  1U>::conditional_value || verify_power_of_two<Digits2 /  3U>::conditional_value
        || verify_power_of_two<Digits2 /  5U>::conditional_value || verify_power_of_two<Digits2 /  7U>::conditional_value
        || verify_power_of_two<Digits2 /  9U>::conditional_value || verify_power_of_two<Digits2 / 11U>::conditional_value
        || verify_power_of_two<Digits2 / 13U>::conditional_value || verify_power_of_two<Digits2 / 15U>::conditional_value
        || verify_power_of_two<Digits2 / 17U>::conditional_value || verify_power_of_two<Digits2 / 19U>::conditional_value
        || verify_power_of_two<Digits2 / 21U>::conditional_value || verify_power_of_two<Digits2 / 23U>::conditional_value
        || verify_power_of_two<Digits2 / 25U>::conditional_value || verify_power_of_two<Digits2 / 27U>::conditional_value
        || verify_power_of_two<Digits2 / 29U>::conditional_value || verify_power_of_two<Digits2 / 31U>::conditional_value
        || verify_power_of_two<Digits2 / 33U>::conditional_value || verify_power_of_two<Digits2 / 35U>::conditional_value
        || verify_power_of_two<Digits2 / 37U>::conditional_value || verify_power_of_two<Digits2 / 39U>::conditional_value
        || verify_power_of_two<Digits2 / 41U>::conditional_value || verify_power_of_two<Digits2 / 43U>::conditional_value
        || verify_power_of_two<Digits2 / 45U>::conditional_value || verify_power_of_two<Digits2 / 47U>::conditional_value
        || verify_power_of_two<Digits2 / 49U>::conditional_value || verify_power_of_two<Digits2 / 51U>::conditional_value
        || verify_power_of_two<Digits2 / 53U>::conditional_value || verify_power_of_two<Digits2 / 55U>::conditional_value
        || verify_power_of_two<Digits2 / 57U>::conditional_value || verify_power_of_two<Digits2 / 59U>::conditional_value
        || verify_power_of_two<Digits2 / 61U>::conditional_value || verify_power_of_two<Digits2 / 63U>::conditional_value);
  };

  template<const std::uint_fast32_t BitCount,
           typename EnableType = void>
  struct int_type_helper
  {
    static_assert((   ((BitCount >= 8U) && (BitCount <= 64U))
                   && (verify_power_of_two<BitCount>::conditional_value == true)),
                  "Error: int_type_helper is not intended to be used for this BitCount");

    using exact_unsigned_type = std::uintmax_t;
    using exact_signed_type   = std::intmax_t;
  };

  template<const std::uint_fast32_t BitCount> struct int_type_helper<BitCount, typename std::enable_if<                     (BitCount <=  8U)>::type> { using exact_unsigned_type = std::uint8_t;  using exact_signed_type = std::int8_t;   };
  template<const std::uint_fast32_t BitCount> struct int_type_helper<BitCount, typename std::enable_if<(BitCount >=  9U) && (BitCount <= 16U)>::type> { using exact_unsigned_type = std::uint16_t; using exact_signed_type = std::int16_t;   };
  template<const std::uint_fast32_t BitCount> struct int_type_helper<BitCount, typename std::enable_if<(BitCount >= 17U) && (BitCount <= 32U)>::type> { using exact_unsigned_type = std::uint32_t; using exact_signed_type = std::int32_t;   };
  template<const std::uint_fast32_t BitCount> struct int_type_helper<BitCount, typename std::enable_if<(BitCount >= 33U) && (BitCount <= 64U)>::type> { using exact_unsigned_type = std::uint64_t; using exact_signed_type = std::int64_t;   };

  // Use a local implementation of string copy.
  inline char* strcpy_unsafe(char* dst, const char* src)
  {
    while((*dst++ = *src++) != char('\0')) { ; }

    return dst;
  }

  // Use a local implementation of string length.
  inline std::uint_fast32_t strlen_unsafe(const char* p_str)
  {
    const char* p_str_copy;

    for(p_str_copy = p_str; (*p_str_copy != char('\0')); ++p_str_copy) { ; }

    return std::uint_fast32_t(p_str_copy - p_str);
  }

  template<typename ST,
           typename LT = typename detail::int_type_helper<std::uint_fast32_t(std::numeric_limits<ST>::digits * 2)>::exact_unsigned_type>
  ST make_lo(const LT& u)
  {
    // From an unsigned integral input parameter of type LT,
    // extract the low part of it. The type of the extracted
    // low part is ST, which has half the width of LT.

    using local_ushort_type = ST;
    using local_ularge_type = LT;

    // Compile-time checks.
    static_assert((    (std::numeric_limits<local_ushort_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ularge_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ushort_type>::is_signed  == false)
                   &&  (std::numeric_limits<local_ularge_type>::is_signed  == false)
                   && ((std::numeric_limits<local_ushort_type>::digits * 2) == std::numeric_limits<local_ularge_type>::digits)),
                   "Error: Please check the characteristics of the template parameters ST and LT");

    return static_cast<local_ushort_type>(u);
  }

  template<typename ST,
           typename LT = typename detail::int_type_helper<std::uint_fast32_t(std::numeric_limits<ST>::digits * 2)>::exact_unsigned_type>
  ST make_hi(const LT& u)
  {
    // From an unsigned integral input parameter of type LT,
    // extract the high part of it. The type of the extracted
    // high part is ST, which has half the width of LT.

    using local_ushort_type = ST;
    using local_ularge_type = LT;

    // Compile-time checks.
    static_assert((    (std::numeric_limits<local_ushort_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ularge_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ushort_type>::is_signed  == false)
                   &&  (std::numeric_limits<local_ularge_type>::is_signed  == false)
                   && ((std::numeric_limits<local_ushort_type>::digits * 2) == std::numeric_limits<local_ularge_type>::digits)),
                   "Error: Please check the characteristics of the template parameters ST and LT");

    return static_cast<local_ushort_type>(u >> std::numeric_limits<local_ushort_type>::digits);
  }

  template<typename ST,
           typename LT = typename detail::int_type_helper<std::uint_fast32_t(std::numeric_limits<ST>::digits * 2)>::exact_unsigned_type>
  LT make_large(const ST& lo, const ST& hi)
  {
    // Create a composite unsigned integral value having type LT.
    // Two constituents are used having type ST, whereby the
    // width of ST is half the width of LT.

    using local_ushort_type = ST;
    using local_ularge_type = LT;

    // Compile-time checks.
    static_assert((    (std::numeric_limits<local_ushort_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ularge_type>::is_integer == true)
                   &&  (std::numeric_limits<local_ushort_type>::is_signed  == false)
                   &&  (std::numeric_limits<local_ularge_type>::is_signed  == false)
                   && ((std::numeric_limits<local_ushort_type>::digits * 2) == std::numeric_limits<local_ularge_type>::digits)),
                   "Error: Please check the characteristics of the template parameters ST and LT");

    return local_ularge_type(local_ularge_type(static_cast<local_ularge_type>(hi) << std::numeric_limits<ST>::digits) | lo);
  }

  } } } // namespace wide_integer::generic_template::detail

  namespace wide_integer { namespace generic_template {

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  class uintwide_t
  {
  public:
    // Class-local type definitions.
    using limb_type        = LimbType;
    using double_limb_type = typename detail::int_type_helper<std::uint_fast32_t(std::numeric_limits<limb_type>::digits * 2)>::exact_unsigned_type;

    // Legacy ularge and ushort types. These are no longer used
    // in the class, but provided for legacy compatibility.
    using ushort_type = limb_type;
    using ularge_type = double_limb_type;

    // More compile-time checks.
    static_assert((    (std::numeric_limits<limb_type>::is_integer == true)
                   &&  (std::numeric_limits<double_limb_type>::is_integer == true)
                   &&  (std::numeric_limits<limb_type>::is_signed  == false)
                   &&  (std::numeric_limits<double_limb_type>::is_signed  == false)
                   && ((std::numeric_limits<limb_type>::digits * 2) == std::numeric_limits<double_limb_type>::digits)),
                   "Error: Please check the characteristics of the template parameters ST and LT");

    // Helper constants for the digit characteristics.
    static constexpr std::uint_fast32_t my_digits   = Digits2;
    static constexpr std::uint_fast32_t my_digits10 = static_cast<int>((std::uintmax_t(my_digits) * UINTMAX_C(301)) / 1000U);

    // The number of limbs.
    static constexpr std::uint_fast32_t number_of_limbs =
      std::uint_fast32_t(my_digits / std::uint_fast32_t(std::numeric_limits<limb_type>::digits));

    static constexpr std::uint_fast32_t number_of_limbs_karatsuba_threshold = std::uint_fast32_t(128U + 1U);

    // Verify that the Digits2 template parameter (my_digits):
    //   * Is equal to 2^n times 1...63.
    //   * And that there are at least 16, 24 or 32 binary digits.
    //   * And that the number of binary digits is an exact multiple of the number of limbs.
    static_assert(   (detail::verify_power_of_two_times_granularity_one_sixty_fourth<my_digits>::conditional_value == true)
                  && ((my_digits >= 16U) || (my_digits >= 24U))
                  && (my_digits == (number_of_limbs * std::uint_fast32_t(std::numeric_limits<limb_type>::digits))),
                  "Error: Digits2 must be 2^n times 1...63 (with n >= 3), while being 16, 24, 32 or larger, and exactly divisible by limb count");

    // The type of the internal data representation.
    using representation_type = std::array<limb_type, number_of_limbs>;

    // The iterator types of the internal data representation.
    using iterator               = typename std::array<limb_type, number_of_limbs>::iterator;
    using const_iterator         = typename std::array<limb_type, number_of_limbs>::const_iterator;
    using reverse_iterator       = typename std::array<limb_type, number_of_limbs>::reverse_iterator;
    using const_reverse_iterator = typename std::array<limb_type, number_of_limbs>::const_reverse_iterator;

    // Define a class-local type that has double the width of *this.
    using double_width_type = uintwide_t<my_digits * 2U, limb_type>;

    // Default constructor.
    uintwide_t() = default;

    // Constructors from built-in unsigned integral types that
    // are less wide than limb_type or exactly as wide as limb_type.
    template<typename UnsignedIntegralType>
    uintwide_t(const UnsignedIntegralType v,
               typename std::enable_if<(   (std::is_fundamental<UnsignedIntegralType>::value == true)
                                        && (std::is_integral   <UnsignedIntegralType>::value == true)
                                        && (std::is_unsigned   <UnsignedIntegralType>::value == true)
                                        && (std::numeric_limits<UnsignedIntegralType>::digits <= std::numeric_limits<limb_type>::digits))>::type* = nullptr)
    {
      values[0U] = limb_type(v);

      std::fill(values.begin() + 1U, values.end(), limb_type(0U));
    }

    // Constructors from built-in unsigned integral types that
    // are wider than limb_type, and do not have exactly the
    // same width as limb_type.
    template<typename UnsignedIntegralType>
    uintwide_t(const UnsignedIntegralType v,
               typename std::enable_if<(   (std::is_fundamental<UnsignedIntegralType>::value == true)
                                        && (std::is_integral   <UnsignedIntegralType>::value == true)
                                        && (std::is_unsigned   <UnsignedIntegralType>::value == true)
                                        && (std::numeric_limits<UnsignedIntegralType>::digits > std::numeric_limits<limb_type>::digits))>::type* = nullptr)
    {
      std::uint_fast32_t right_shift_amount_v = 0U;
      std::uint_fast8_t  index_u              = 0U;

      for( ; (index_u < values.size()) && (right_shift_amount_v < std::uint_fast32_t(std::numeric_limits<UnsignedIntegralType>::digits)); ++index_u)
      {
        values[index_u] = limb_type(v >> (int) right_shift_amount_v);

        right_shift_amount_v += std::uint_fast32_t(std::numeric_limits<limb_type>::digits);
      }

      std::fill(values.begin() + index_u, values.end(), limb_type(0U));
    }

    // Constructors from built-in signed integral types.
    template<typename SignedIntegralType>
    uintwide_t(const SignedIntegralType v,
               typename std::enable_if<(   (std::is_fundamental<SignedIntegralType>::value == true)
                                        && (std::is_integral   <SignedIntegralType>::value == true)
                                        && (std::is_signed     <SignedIntegralType>::value == true))>::type* = nullptr)
    {
      using local_signed_integral_type   = SignedIntegralType;
      using local_unsigned_integral_type = typename detail::int_type_helper<std::numeric_limits<local_signed_integral_type>::digits + 1>::exact_unsigned_type;

      const bool is_neg = (v < local_signed_integral_type(0));

      const local_unsigned_integral_type u =
        ((is_neg == false) ? local_unsigned_integral_type(v) : local_unsigned_integral_type(-v));

      operator=(uintwide_t(u));

      if(is_neg) { negate(); }
    }

    // Constructor from the internal data representation.
    uintwide_t(const representation_type& other_rep)
    {
      std::copy(other_rep.cbegin(), other_rep.cend(), values.begin());
    }

    // Constructor from initializer list of limbs.
    uintwide_t(std::initializer_list<limb_type> lst)
    {
      const std::uint_fast32_t sz = (std::min)(std::uint_fast32_t(lst.size()),
                                               std::uint_fast32_t(values.size()));

      std::copy(lst.begin(), lst.begin() + sz, values.begin());
      std::fill(values.begin() + sz, values.end(), limb_type(0U));
    }

    // Constructor from a C-style array.
    template<const std::uint_fast32_t N>
    uintwide_t(const limb_type(&init)[N])
    {
      static_assert(N <= number_of_limbs,
                    "Error: The initialization list has too many elements.");

      std::copy(init, init + (std::min)(N, number_of_limbs), values.begin());
    }

    // Copy constructor.
    uintwide_t(const uintwide_t& other) : values(other.values) { }

    // Constructor from the double-width type.
    // This constructor is explicit because it
    // is a narrowing conversion.
    template<typename UnknownUnsignedWideIntegralType = double_width_type>
    explicit uintwide_t(const UnknownUnsignedWideIntegralType& v,
                        typename std::enable_if<(   (std::is_same<UnknownUnsignedWideIntegralType, double_width_type>::value == true)
                                                 && (128U <= my_digits))>::type* = nullptr)
    {
      std::copy(v.crepresentation().cbegin(),
                v.crepresentation().cbegin() + (v.crepresentation().size() / 2U),
                values.begin());
    }

    // Constructor from the another type having a different width but the same limb type.
    // This constructor is explicit because it is a non-trivial conversion.
    template<const std::uint_fast32_t OtherDigits2>
    explicit uintwide_t(const uintwide_t<OtherDigits2, LimbType>& v)
    {
      if(v.crepresentation().size() > values.size())
      {
        std::copy(v.crepresentation().cbegin(),
                  v.crepresentation().cbegin() + values.size(),
                  values.begin());
      }
      else if(v.crepresentation().size() <= values.size())
      {
        std::copy(v.crepresentation().cbegin(),
                  v.crepresentation().cend(),
                  values.begin());

        std::fill(values.begin() + v.crepresentation().size(),
                  values.end(),
                  limb_type(0U));
      }
      else
      {
        values.fill(0U);
      }
    }

    // Constructor from a constant character string.
    uintwide_t(const char* str_input)
    {
      if(rd_string(str_input) == false)
      {
        std::fill(values.begin(), values.end(), (std::numeric_limits<limb_type>::max)());
      }
    }

    // Move constructor.
    uintwide_t(uintwide_t&& other) : values(static_cast<representation_type&&>(other.values)) { }

    // Default destructor.
    ~uintwide_t() = default;

    // Assignment operator.
    uintwide_t& operator=(const uintwide_t& other)
    {
      if(this != &other)
      {
        std::copy(other.values.cbegin(), other.values.cend(), values.begin());
      }

      return *this;
    }

    // Trivial move assignment operator.
    uintwide_t& operator=(uintwide_t&& other)
    {
      std::copy(other.values.cbegin(), other.values.cend(), values.begin());

      return *this;
    }

    // Implement a cast operator that casts to any
    // built-in signed or unsigned integral type.
    template<typename UnknownBuiltInIntegralType,
             typename = typename std::enable_if<
                          (   (std::is_fundamental<UnknownBuiltInIntegralType>::value == true)
                           && (std::is_integral   <UnknownBuiltInIntegralType>::value == true))>::type>
    explicit operator UnknownBuiltInIntegralType() const
    {
      using local_unknown_integral_type  = UnknownBuiltInIntegralType;

      using local_unsigned_integral_type =
        typename detail::int_type_helper<
          std::numeric_limits<local_unknown_integral_type>::is_signed
            ? std::numeric_limits<local_unknown_integral_type>::digits + 1
            : std::numeric_limits<local_unknown_integral_type>::digits + 0>::exact_unsigned_type;

      local_unsigned_integral_type cast_result;

      const std::uint_fast8_t digits_ratio =
        std::uint_fast8_t(  std::numeric_limits<local_unsigned_integral_type>::digits
                          / std::numeric_limits<limb_type>::digits);

      switch(digits_ratio)
      {
        case 0:
        case 1:
          // The input parameter is less wide or equally as wide as the limb width.
          cast_result = static_cast<local_unsigned_integral_type>(values[0U]);
          break;

        default:
          // The input parameter is wider than the limb width.
          cast_result = 0U;

          for(std::uint_fast8_t i = 0U; i < digits_ratio; ++i)
          {
            const local_unsigned_integral_type u =
              local_unsigned_integral_type(values[i]) << (std::numeric_limits<limb_type>::digits * int(i));

            cast_result |= u;
          }
          break;
      }

      return local_unknown_integral_type(cast_result);
    }

    // Implement the cast operator that casts to the double-width type.
    template<typename UnknownUnsignedWideIntegralType = double_width_type,
             typename = typename std::enable_if<(std::is_same<UnknownUnsignedWideIntegralType, double_width_type>::value == true)>::type>
    operator double_width_type() const
    {
      double_width_type local_double_width_instance;

      std::copy(values.cbegin(),
                values.cend(),
                local_double_width_instance.representation().begin());

      std::fill(local_double_width_instance.representation().begin() + number_of_limbs,
                local_double_width_instance.representation().end(),
                limb_type(0U));

      return local_double_width_instance;
    }

    // Provide a user interface to the internal data representation.
          representation_type&  representation()       { return values; }
    const representation_type&  representation() const { return values; }
    const representation_type& crepresentation() const { return values; }

    // Unary operators: not, plus and minus.
    const uintwide_t& operator+() const { return *this; }
          uintwide_t  operator-() const { uintwide_t tmp(*this); tmp.negate(); return tmp; }

    uintwide_t& operator+=(const uintwide_t& other)
    {
      if(this == &other)
      {
        return operator+=(uintwide_t(other));
      }
      else
      {
        // Unary addition function.
        const limb_type carry = eval_add_n(values.data(),
                                             values.data(),
                                             other.values.data(),
                                             number_of_limbs,
                                             limb_type(0U));

        static_cast<void>(carry);

        return *this;
      }
    }

    uintwide_t& operator-=(const uintwide_t& other)
    {
      if(this == &other)
      {
        values.fill(0U);

        return *this;
      }
      else
      {
        // Unary subtraction function.
        const limb_type has_borrow = eval_subtract_n(values.data(),
                                                       values.data(),
                                                       other.values.data(),
                                                       number_of_limbs,
                                                       false);

        static_cast<void>(has_borrow);

        return *this;
      }
    }

    uintwide_t& operator*=(const uintwide_t& other)
    {
      if(this == &other)
      {
        return operator*=(uintwide_t(other));
      }
      else
      {
        eval_mul_unary(*this, other);

        return *this;
      }
    }

    uintwide_t& mul_by_limb(const limb_type v)
    {
      if(v == 0U)
      {
        values.fill(0U);

        return *this;
      }
      else
      {
        std::array<limb_type, number_of_limbs> result;

        const limb_type carry = eval_multiply_1d(result.data(),
                                                   values.data(),
                                                   v,
                                                   number_of_limbs);

        static_cast<void>(carry);

        std::copy(result.cbegin(),
                  result.cbegin() + number_of_limbs,
                  values.begin());

        return *this;
      }
    }

    uintwide_t& operator/=(const uintwide_t& other)
    {
      if(this == &other)
      {
        values.front() = 1U;

        std::fill(values.begin() + 1U, values.end(), limb_type(0U));

        return *this;
      }
      else if(other.is_zero())
      {
        values.fill((std::numeric_limits<limb_type>::max)());

        return *this;
      }
      else
      {
        // Unary division function.
        eval_divide_knuth(other, nullptr);

        return *this;
      }
    }

    uintwide_t& operator%=(const uintwide_t& other)
    {
      if(this == &other)
      {
        std::fill(values.begin(), values.end(), limb_type(0U));

        return *this;
      }
      else
      {
        // Unary modulus function.
        uintwide_t remainder;

        eval_divide_knuth(other, &remainder);

        values = remainder.values;

        return *this;
      }
    }

    // Operators pre-increment and pre-decrement.
    uintwide_t& operator++() { preincrement(); return *this; }
    uintwide_t& operator--() { predecrement(); return *this; }

    // Operators post-increment and post-decrement.
    uintwide_t operator++(int) { const uintwide_t w(*this); preincrement(); return w; }
    uintwide_t operator--(int) { const uintwide_t w(*this); predecrement(); return w; }

    uintwide_t& operator~()
    {
      // Bitwise NOT.
      bitwise_not();

      return *this;
    }

    uintwide_t& operator|=(const uintwide_t& other)
    {
      if(this == &other)
      {
        return *this;
      }
      else
      {
        // Bitwise OR.
        for(std::uint_fast32_t i = 0U; i < number_of_limbs; ++i)
        {
          values[i] |= other.values[i];
        }

        return *this;
      }
    }

    uintwide_t& operator^=(const uintwide_t& other)
    {
      if(this == &other)
      {
        values.fill(0U);

        return *this;
      }
      else
      {
        // Bitwise XOR.
        for(std::uint_fast32_t i = 0U; i < number_of_limbs; ++i)
        {
          values[i] ^= other.values[i];
        }

        return *this;
      }
    }

    uintwide_t& operator&=(const uintwide_t& other)
    {
      if(this == &other)
      {
        return *this;
      }
      else
      {
        // Bitwise AND.
        for(std::uint_fast32_t i = 0U; i < number_of_limbs; ++i)
        {
          values[i] &= other.values[i];
        }

        return *this;
      }
    }

    template<typename IntegralType>
    typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                             && (std::is_integral   <IntegralType>::value == true)), uintwide_t>::type&
    operator<<=(const IntegralType n)
    {
      // Left-shift operator.
      if     (n <  0) { operator>>=(n); }
      else if(n == 0) { ; }
      else
      {
        if(std::uint_fast32_t(n) >= my_digits)
        {
          std::fill(values.begin(), values.end(), limb_type(0U));
        }
        else
        {
          const std::uint_fast32_t offset            = std::uint_fast32_t(n) / std::uint_fast32_t(std::numeric_limits<limb_type>::digits);
          const std::uint_fast32_t left_shift_amount = std::uint_fast32_t(n) % std::uint_fast32_t(std::numeric_limits<limb_type>::digits);

          if(offset > 0U)
          {
            std::copy_backward(values.data(),
                               values.data() + (number_of_limbs - offset),
                               values.data() +  number_of_limbs);

            std::fill(values.begin(), values.begin() + offset, limb_type(0U));
          }

          limb_type part_from_previous_value = limb_type(0U);

          using local_integral_type = IntegralType;

          if(left_shift_amount != local_integral_type(0U))
          {
            for(std::uint_fast32_t i = offset; i < number_of_limbs; ++i)
            {
              const limb_type t = values[i];

              values[i] = (t << local_integral_type(left_shift_amount)) | part_from_previous_value;

              part_from_previous_value = limb_type(t >> local_integral_type(std::uint_fast32_t(std::numeric_limits<limb_type>::digits - left_shift_amount)));
            }
          }
        }
      }

      return *this;
    }

    template<typename IntegralType>
    typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                             && (std::is_integral   <IntegralType>::value == true)), uintwide_t>::type&
    operator>>=(const IntegralType n)
    {
      // Right-shift operator.
      if     (n <  0) { operator<<=(n); }
      else if(n == 0) { ; }
      else
      {
        if(std::uint_fast32_t(n) >= my_digits)
        {
          std::fill(values.begin(), values.end(), limb_type(0U));
        }
        else
        {
          const std::uint_fast32_t offset             = std::uint_fast32_t(n) / std::uint_fast32_t(std::numeric_limits<limb_type>::digits);
          const std::uint_fast32_t right_shift_amount = std::uint_fast32_t(n) % std::uint_fast32_t(std::numeric_limits<limb_type>::digits);

          if(offset > 0U)
          {
            std::copy(values.begin() + offset,
                      values.begin() + number_of_limbs,
                      values.begin());

            std::fill(values.end() - offset, values.end(), limb_type(0U));
          }

          limb_type part_from_previous_value = limb_type(0U);

          using local_integral_type = IntegralType;

          if(right_shift_amount != local_integral_type(0U))
          {
            for(std::int_fast32_t i = std::int_fast32_t((number_of_limbs - 1U) - offset); i >= 0; --i)
            {
              const limb_type t = values[std::uint_fast32_t(i)];

              values[std::uint_fast32_t(i)] = (t >> local_integral_type(right_shift_amount)) | part_from_previous_value;

              part_from_previous_value = limb_type(t << local_integral_type(std::uint_fast32_t(std::numeric_limits<limb_type>::digits - right_shift_amount)));
            }
          }
        }
      }

      return *this;
    }

    // Implement comparison operators.
    bool operator==(const uintwide_t& other) const { return (compare(other) == std::int_fast8_t( 0)); }
    bool operator< (const uintwide_t& other) const { return (compare(other) == std::int_fast8_t(-1)); }
    bool operator> (const uintwide_t& other) const { return (compare(other) == std::int_fast8_t( 1)); }
    bool operator!=(const uintwide_t& other) const { return (compare(other) != std::int_fast8_t( 0)); }
    bool operator<=(const uintwide_t& other) const { return (compare(other) <= std::int_fast8_t( 0)); }
    bool operator>=(const uintwide_t& other) const { return (compare(other) >= std::int_fast8_t( 0)); }

    // Helper functions for supporting std::numeric_limits<>.
    static uintwide_t limits_helper_max()
    {
      uintwide_t val;

      std::fill(val.values.begin(),
                val.values.end(),
                (std::numeric_limits<limb_type>::max)());

      return val;
    }

    static uintwide_t limits_helper_min()
    {
      return uintwide_t(std::uint8_t(0U));
    }

    // Define the maximum buffer sizes for extracting
    // octal, decimal and hexadecimal string representations.
    static constexpr std::uint_fast32_t wr_string_max_buffer_size_oct = (16U + (my_digits / 3U)) + std::uint_fast32_t(((my_digits % 3U) != 0U) ? 1U : 0U) + 1U;
    static constexpr std::uint_fast32_t wr_string_max_buffer_size_hex = (32U + (my_digits / 4U)) + 1U;
    static constexpr std::uint_fast32_t wr_string_max_buffer_size_dec = (20U + std::uint_fast32_t((std::uintmax_t(my_digits) * UINTMAX_C(301)) / UINTMAX_C(1000))) + 1U;

    // Write string function.
    bool wr_string(      char*             str_result,
                   const std::uint_fast8_t base_rep     = 0x10U,
                   const bool              show_base    = true,
                   const bool              show_pos     = false,
                   const bool              is_uppercase = true,
                         std::uint_fast32_t       field_width  = 0U,
                   const char              fill_char    = char('0')) const
    {
      uintwide_t t(*this);

      bool wr_string_is_ok = true;

      if(base_rep == 8U)
      {
        const limb_type mask(std::uint8_t(0x7U));

        char str_temp[wr_string_max_buffer_size_oct];

        std::uint_fast32_t pos = (sizeof(str_temp) - 1U);

        if(t.is_zero())
        {
          --pos;

          str_temp[pos] = char('0');
        }
        else
        {
          while(t.is_zero() == false)
          {
            char c = char(t.values[0U] & mask);

            if(c <= 8) { c += char(0x30); }

            --pos;

            str_temp[pos] = c;

            t >>= 3;
          }
        }

        if(show_base)
        {
          --pos;

          str_temp[pos] = char('0');
        }

        if(show_pos)
        {
          --pos;

          str_temp[pos] = char('+');
        }

        if(field_width != 0U)
        {
          field_width = (std::min)(field_width, std::uint_fast32_t(sizeof(str_temp) - 1U));

          while(std::int_fast32_t(pos) > std::int_fast32_t((sizeof(str_temp) - 1U) - field_width))
          {
            --pos;

            str_temp[pos] = fill_char;
          }
        }

        str_temp[(sizeof(str_temp) - 1U)] = char('\0');

        detail::strcpy_unsafe(str_result, str_temp + pos);
      }
      else if(base_rep == 10U)
      {
        char str_temp[wr_string_max_buffer_size_dec];

        std::uint_fast32_t pos = (sizeof(str_temp) - 1U);

        if(t.is_zero() == true)
        {
          --pos;

          str_temp[pos] = char('0');
        }
        else
        {
          const uintwide_t ten(std::uint8_t(10U));

          while(t.is_zero() == false)
          {
            // TBD: Try to generally improve efficiency and reduce
            // the number of temporaries and the count of operations
            // on them in the conversion to decimal string.

            const uintwide_t t_temp(t);

            t /= ten;

            char c = char((t_temp - (uintwide_t(t).mul_by_limb(10U))).values[0U]);

            if(c <= char(9)) { c += char(0x30); }

            --pos;

            str_temp[pos] = c;
          }
        }

        if(show_pos)
        {
          --pos;

          str_temp[pos] = char('+');
        }

        if(field_width != 0U)
        {
          field_width = (std::min)(field_width, std::uint_fast32_t(sizeof(str_temp) - 1U));

          while(std::int_fast32_t(pos) > std::int_fast32_t((sizeof(str_temp) - 1U) - field_width))
          {
            --pos;

            str_temp[pos] = fill_char;
          }
        }

        str_temp[(sizeof(str_temp) - 1U)] = char('\0');

        detail::strcpy_unsafe(str_result, str_temp + pos);
      }
      else if(base_rep == 16U)
      {
        const limb_type mask(std::uint8_t(0xFU));

        char str_temp[wr_string_max_buffer_size_hex];

        std::uint_fast32_t pos = (sizeof(str_temp) - 1U);

        if(t.is_zero() == true)
        {
          --pos;

          str_temp[pos] = char('0');
        }
        else
        {
          while(t.is_zero() == false)
          {
            char c(t.values[0U] & mask);

            if      (c <= char(  9))                      { c += char(0x30); }
            else if((c >= char(0xA)) && (c <= char(0xF))) { c += (is_uppercase ? char(55) : char(87)); }

            --pos;

            str_temp[pos] = c;

            t >>= 4;
          }
        }

        if(show_base)
        {
          --pos;

          str_temp[pos] = (is_uppercase ? char('X') : char('x'));

          --pos;

          str_temp[pos] = char('0');
        }

        if(show_pos)
        {
          --pos;

          str_temp[pos] = char('+');
        }

        if(field_width != 0U)
        {
          field_width = (std::min)(field_width, std::uint_fast32_t(sizeof(str_temp) - 1U));

          while(std::int_fast32_t(pos) > std::int_fast32_t((sizeof(str_temp) - 1U) - field_width))
          {
            --pos;

            str_temp[pos] = fill_char;
          }
        }

        str_temp[(sizeof(str_temp) - 1U)] = char('\0');

        detail::strcpy_unsafe(str_result, str_temp + pos);
      }
      else
      {
        wr_string_is_ok = false;
      }

      return wr_string_is_ok;
    }

    std::int_fast8_t compare(const uintwide_t& other) const
    {
      const std::int_fast8_t cmp_result = compare_ranges(values.data(), other.values.data(), number_of_limbs);

      return cmp_result;
    }

    void negate()
    {
      bitwise_not();

      preincrement();
    }

    void eval_divide_by_single_limb(const limb_type short_denominator, const std::uint_fast32_t u_offset, uintwide_t* remainder)
    {
      // The denominator has one single limb.
      // Use a one-dimensional division algorithm.

      double_limb_type long_numerator    = double_limb_type(0U);

      limb_type hi_part = limb_type(0U);

      for(std::int_fast32_t i = std::int_fast32_t((number_of_limbs - 1U) - u_offset); std::int_fast32_t(i) >= 0; --i)
      {
        long_numerator =
            double_limb_type(values[std::uint_fast32_t(i)])
          + ((long_numerator - double_limb_type(double_limb_type(short_denominator) * hi_part)) << std::numeric_limits<limb_type>::digits);

        values[std::uint_fast32_t(i)] =
          detail::make_lo<limb_type>(double_limb_type(long_numerator / short_denominator));

        hi_part = values[std::uint_fast32_t(i)];
      }

      if(remainder != nullptr)
      {
        long_numerator = double_limb_type(values[0U]) + ((long_numerator - double_limb_type(double_limb_type(short_denominator) * hi_part)) << std::numeric_limits<limb_type>::digits);

        *remainder = limb_type(long_numerator >> std::numeric_limits<limb_type>::digits);
      }
    }

  private:
    representation_type values;

    static std::int_fast8_t compare_ranges(const limb_type* a, const limb_type* b, const std::uint_fast32_t count)
    {
      std::int_fast8_t cmp_result;

      std::int_fast32_t element_index = std::int_fast32_t(count) - 1;

      while((   (element_index >= 0)
             && (a[(element_index)] == b[(element_index)])))
      {
        --element_index;
      }

      if(element_index == std::int_fast32_t(-1))
      {
        cmp_result = std::int_fast8_t(0);
      }
      else
      {
        const bool left_is_greater_than_right =
          (a[(element_index)] > b[(element_index)]);

        cmp_result = (left_is_greater_than_right ? std::int_fast8_t(1) : std::int_fast8_t(-1));
      }

      return cmp_result;
    }

    template<const std::uint_fast32_t OtherDigits2>
    static void eval_mul_unary(      uintwide_t<OtherDigits2, LimbType>& u,
                               const uintwide_t<OtherDigits2, LimbType>& v,
                               typename std::enable_if<((OtherDigits2 / std::numeric_limits<LimbType>::digits) < uintwide_t::number_of_limbs_karatsuba_threshold)>::type* = nullptr)
    {
      // Unary multiplication function using schoolbook multiplication,
      // but we only need to retain the low half of the n*n algorithm.
      // In other words, this is an n*n->n bit multiplication.

      constexpr std::uint_fast32_t local_number_of_limbs = uintwide_t<OtherDigits2, LimbType>::number_of_limbs;

      std::array<limb_type, local_number_of_limbs> result;

      eval_multiply_n_by_n_to_lo_part(result.data(),
                                      u.values.data(),
                                      v.values.data(),
                                      local_number_of_limbs);

      std::copy(result.cbegin(),
                result.cbegin() + local_number_of_limbs,
                u.values.begin());
    }

    template<const std::uint_fast32_t OtherDigits2>
    static void eval_mul_unary(      uintwide_t<OtherDigits2, LimbType>& u,
                               const uintwide_t<OtherDigits2, LimbType>& v,
                               typename std::enable_if<((OtherDigits2 / std::numeric_limits<LimbType>::digits) >= uintwide_t::number_of_limbs_karatsuba_threshold)>::type* = nullptr)
    {
      // Unary multiplication function using Karatsuba multiplication.

      constexpr std::uint_fast32_t local_number_of_limbs = uintwide_t<OtherDigits2, LimbType>::number_of_limbs;

      std::array<limb_type, local_number_of_limbs * 2U> result;
      std::array<limb_type, local_number_of_limbs * 4U> t;

      eval_multiply_kara_n_by_n_to_2n(result.data(),
                                      u.values.data(),
                                      v.values.data(),
                                      local_number_of_limbs,
                                      t.data());

      std::copy(result.cbegin(),
                result.cbegin() + local_number_of_limbs,
                u.values.begin());
    }

    static limb_type eval_add_n(      limb_type* r,
                                  const limb_type* u,
                                  const limb_type* v,
                                  const std::uint_fast32_t  count,
                                  const limb_type  carry_in = 0U)
    {
      limb_type carry_out = carry_in;

      for(std::uint_fast32_t i = 0U; i < count; ++i)
      {
        const double_limb_type uv_as_ularge = double_limb_type(double_limb_type(u[i]) + v[i]) + carry_out;

        carry_out = detail::make_hi<limb_type>(uv_as_ularge);

        r[i] = limb_type(uv_as_ularge);
      }

      return carry_out;
    }

    static bool eval_subtract_n(      limb_type* r,
                                const limb_type* u,
                                const limb_type* v,
                                const std::uint_fast32_t  count,
                                const bool         has_borrow_in = false)
    {
      bool has_borrow_out = has_borrow_in;

      for(std::uint_fast32_t i = 0U; i < count; ++i)
      {
        double_limb_type uv_as_ularge = double_limb_type(u[i]) - v[i];

        if(has_borrow_out)
        {
          --uv_as_ularge;
        }

        has_borrow_out = (detail::make_hi<limb_type>(uv_as_ularge) != limb_type(0U));

        r[i] = limb_type(uv_as_ularge);
      }

      return has_borrow_out;
    }

    static void eval_multiply_n_by_n_to_lo_part(      limb_type*       r,
                                                const limb_type*       a,
                                                const limb_type*       b,
                                                const std::uint_fast32_t count)
    {
      std::memset(r, 0, count * sizeof(limb_type));

      for(std::uint_fast32_t i = 0U; i < count; ++i)
      {
        if(a[i] != limb_type(0U))
        {
          double_limb_type carry = 0U;

          for(std::uint_fast32_t j = 0U; j < (count - i); ++j)
          {
            carry += double_limb_type(double_limb_type(a[i]) * b[j]);
            carry += r[i + j];

            r[i + j] = detail::make_lo<limb_type>(carry);
            carry    = detail::make_hi<limb_type>(carry);
          }
        }
      }
    }

    static void eval_multiply_n_by_n_to_2n(      limb_type*       r,
                                           const limb_type*       a,
                                           const limb_type*       b,
                                           const std::uint_fast32_t count)
    {
      std::memset(r, 0, (count * 2U) * sizeof(limb_type));

      for(std::uint_fast32_t i = 0U; i < count; ++i)
      {
        if(a[i] != limb_type(0U))
        {
          std::uint_fast32_t j = 0U;

          double_limb_type carry = 0U;

          for( ; j < count; ++j)
          {
            carry += double_limb_type(double_limb_type(a[i]) * b[j]);
            carry += r[i + j];

            r[i + j] = detail::make_lo<limb_type>(carry);
            carry    = detail::make_hi<limb_type>(carry);
          }

          r[i + j] = limb_type(carry);
        }
      }
    }

    static limb_type eval_multiply_1d(      limb_type*       r,
                                      const limb_type*       a,
                                      const limb_type        b,
                                      const std::uint_fast32_t count)
    {
      std::memset(r, 0, count * sizeof(limb_type));

      double_limb_type carry = 0U;

      if(b != limb_type(0U))
      {
        for(std::uint_fast32_t i = 0U ; i < count; ++i)
        {
          carry += double_limb_type(double_limb_type(a[i]) * b);
          carry += r[i];

          r[i]  = detail::make_lo<limb_type>(carry);
          carry = detail::make_hi<limb_type>(carry);
        }
      }

      return limb_type(carry);
    }

    static void eval_multiply_kara_propagate_carry(limb_type* t, const std::uint_fast32_t n, const limb_type carry)
    {
      std::uint_fast32_t i = 0U;

      limb_type carry_out = carry;

      while((i < n) && (carry_out != 0U))
      {
        const double_limb_type uv_as_ularge = double_limb_type(t[i]) + carry_out;

        carry_out = detail::make_hi<limb_type>(uv_as_ularge);

        t[i] = limb_type(uv_as_ularge);

        ++i;
      }
    }

    static void eval_multiply_kara_propagate_borrow(limb_type* t, const std::uint_fast32_t n, const bool has_borrow)
    {
      std::uint_fast32_t i = 0U;

      bool has_borrow_out = has_borrow;

      while((i < n) && (has_borrow_out == true))
      {
        double_limb_type uv_as_ularge = double_limb_type(t[i]);

        if(has_borrow_out)
        {
          --uv_as_ularge;
        }

        has_borrow_out = (detail::make_hi<limb_type>(uv_as_ularge) != limb_type(0U));

        t[i] = limb_type(uv_as_ularge);

        ++i;
      }
    }

    static void eval_multiply_kara_n_by_n_to_2n(      limb_type*       r,
                                                const limb_type*       a,
                                                const limb_type*       b,
                                                const std::uint_fast32_t n,
                                                      limb_type*       t)
    {
      if((n >= 32U) && (n <= 63U))
      {
        static_cast<void>(t);

        eval_multiply_n_by_n_to_2n(r, a, b, n);
      }
      else
      {
        // Based on "Algorithm 1.3 KaratsubaMultiply", Sect. 1.3.2, page 5
        // of R.P. Brent and P. Zimmermann, "Modern Computer Arithmetic",
        // Cambridge University Press (2011).

        // The Karatsuba multipliation computes the product of u*v as:
        // [b^N + b^(N/2)] a1*b1 + [b^(N/2)](a1 - a0)(b0 - b1) + [b^(N/2) + 1] a0*b0

        // Here we visualize u and v in two components 0,1 corresponding
        // to the high and low order parts, respectively.

        // TBD: A subproblem: Deal with the subproblem getting negative,
        // and consider the overhead for calculating the complements.
        // Boost kara mul branch: z = (a1+a0)*(b1+b0) - a1*b1 - a0*b0
        // This code:             z =  a1*b1 + a0*b0 - |a1-a0|*|b0-b1|
        // This also may seem to be known as odd-even Karatsuba in
        // R.P. Brent and P. Zimmermann, "Modern Computer Arithmetic",
        // Cambridge University Press (2011).

        // Step 1
        // Calculate a1*b1 and store it in the upper part of r.
        // Calculate a0*b0 and store it in the lower part of r.
        // copy r to t0.

        // Step 2
        // Add a1*b1 (which is t2) to the middle two-quarters of r (which is r1)
        // Add a0*b0 (which is t0) to the middle two-quarters of r (which is r1)

        // Step 3
        // Calculate |a1-a0| in t0 and note the sign (i.e., the borrow flag)

        // Step 4
        // Calculate |b0-b1| in t1 and note the sign (i.e., the borrow flag)

        // Step 5
        // Call kara mul to calculate |a1-a0|*|b0-b1| in (t2),
        // while using temporary storage in t4 along the way.

        // Step 6
        // Check the borrow signs. If a1-a0 and b0-b1 have the same signs,
        // then add |a1-a0|*|b0-b1| to r1, otherwise subtract it from r1.

        const std::uint_fast32_t  nh = n / 2U;

        const limb_type* a0 = a + 0U;
        const limb_type* a1 = a + nh;

        const limb_type* b0 = b + 0U;
        const limb_type* b1 = b + nh;

              limb_type* r0 = r + 0U;
              limb_type* r1 = r + nh;
              limb_type* r2 = r + n;
              limb_type* r3 = r + (n + nh);
              limb_type* r4 = r + (n + n);

              limb_type* t0 = t + 0U;
              limb_type* t1 = t + nh;
              limb_type* t2 = t + n;
              limb_type* t4 = t + (n + n);

        // Step 1
        //   a1*b1 -> r2
        //   a0*b0 -> r0
        //   r -> t0
        eval_multiply_kara_n_by_n_to_2n(r2, a1, b1, nh, t0);
        eval_multiply_kara_n_by_n_to_2n(r0, a0, b0, nh, t0);
        std::copy(r0, r4, t0);

        // Step 2
        //   r1 += a1*b1
        //   r1 += a0*b0
        limb_type carry;
        carry = eval_add_n(r1, r1, t2, n);
        eval_multiply_kara_propagate_carry(r3, nh, carry);
        carry = eval_add_n(r1, r1, t0, n);
        eval_multiply_kara_propagate_carry(r3, nh, carry);

        // Step 3
        //   |a1-a0| -> t0
        const std::int_fast8_t cmp_result_a1a0 = compare_ranges(a1, a0, nh);

        if(cmp_result_a1a0 == 1)
        {
          static_cast<void>(eval_subtract_n(t0, a1, a0, nh));
        }
        else if(cmp_result_a1a0 == -1)
        {
          static_cast<void>(eval_subtract_n(t0, a0, a1, nh));
        }

        // Step 4
        //   |b0-b1| -> t1
        const std::int_fast8_t cmp_result_b0b1 = compare_ranges(b0, b1, nh);

        if(cmp_result_b0b1 == 1)
        {
          static_cast<void>(eval_subtract_n(t1, b0, b1, nh));
        }
        else if(cmp_result_b0b1 == -1)
        {
          static_cast<void>(eval_subtract_n(t1, b1, b0, nh));
        }

        // Step 5
        //   |a1-a0|*|b0-b1| -> t2
        eval_multiply_kara_n_by_n_to_2n(t2, t0, t1, nh, t4);

        // Step 6
        //   either r1 += |a1-a0|*|b0-b1|
        //   or     r1 -= |a1-a0|*|b0-b1|
        if((cmp_result_a1a0 * cmp_result_b0b1) == 1)
        {
          carry = eval_add_n(r1, r1, t2, n);

          eval_multiply_kara_propagate_carry(r3, nh, carry);
        }
        else if((cmp_result_a1a0 * cmp_result_b0b1) == -1)
        {
          const bool has_borrow = eval_subtract_n(r1, r1, t2, n);

          eval_multiply_kara_propagate_borrow(r3, nh, has_borrow);
        }
      }
    }

    static void eval_multiply_toomcook3(      limb_type* r,
                                        const limb_type* u,
                                        const limb_type* v,
                                        const std::uint_fast32_t  n,
                                              limb_type* t)
    {
      if(n == 3072U)
      {
        // TBD: Tune the threshold for the transition
        // from Toom-Cook3 back to base case Karatsuba.

        // Base case Karatsuba multiplication.
        eval_multiply_kara_n_by_n_to_2n(r, u, v, n, t);
      }
      else
      {
        // Based on "Algorithm 1.4 ToomCook3", Sect. 1.3.3, page 7 of
        // R.P. Brent and P. Zimmermann, "Modern Computer Arithmetic",
        // Cambridge University Press (2011).

        // TBD: Toom-Cook3
      }
    }

    static void eval_multiply_toomcook4(      limb_type* r,
                                        const limb_type* u,
                                        const limb_type* v,
                                        const std::uint_fast32_t  n,
                                              limb_type* t)
    {
      if(n == 2048U)
      {
        // TBD: Tune the threshold for the transition
        // from Toom-Cook4 back to base case Karatsuba.

        // Base case Karatsuba multiplication.
        eval_multiply_kara_n_by_n_to_2n(r, u, v, n, t);
      }
      else
      {
        // TBD: Toom-Cook4
      }
    }

    void eval_divide_knuth(const uintwide_t& other, uintwide_t* remainder)
    {
      // TBD: Consider cleaning up the unclear flow-control
      // caused by numerous return statements in this subroutine.

      // Use Knuth's long division algorithm.
      // The loop-ordering of indexes in Knuth's original
      // algorithm has been reversed due to the data format
      // used here.

      // See also:
      // D.E. Knuth, "The Art of Computer Programming, Volume 2:
      // Seminumerical Algorithms", Addison-Wesley (1998),
      // Section 4.3.1 Algorithm D and Exercise 16.

      using local_uint_index_type = std::uint_fast32_t;

      local_uint_index_type u_offset = local_uint_index_type(0U);
      local_uint_index_type v_offset = local_uint_index_type(0U);

      // Compute the offsets for u and v.
      for(local_uint_index_type i = 0U; (i < number_of_limbs) && (      values[(number_of_limbs - 1U) - i] == limb_type(0U)); ++i) { ++u_offset; }
      for(local_uint_index_type i = 0U; (i < number_of_limbs) && (other.values[(number_of_limbs - 1U) - i] == limb_type(0U)); ++i) { ++v_offset; }

      if(v_offset == local_uint_index_type(number_of_limbs))
      {
        // The denominator is zero. Set the maximum value and return.
        // This also catches (0 / 0) and sets the maximum value for it.
        operator=(limits_helper_max());

        if(remainder != nullptr)
        {
          *remainder = uintwide_t(std::uint8_t(0U));
        }

        return;
      }

      if(u_offset == local_uint_index_type(number_of_limbs))
      {
        // The numerator is zero. Do nothing and return.

        if(remainder != nullptr)
        {
          *remainder = uintwide_t(std::uint8_t(0U));
        }

        return;
      }

      {
        const int result_of_compare_left_with_right = compare(other);

        const bool left_is_less_than_right = (result_of_compare_left_with_right == -1);
        const bool left_is_equal_to_right  = (result_of_compare_left_with_right ==  0);

        if(left_is_less_than_right)
        {
          // If the denominator is larger than the numerator,
          // then the result of the division is zero.
          if(remainder != nullptr)
          {
            *remainder = *this;
          }

          operator=(std::uint8_t(0U));

          return;
        }

        if(left_is_equal_to_right)
        {
          // If the denominator is equal to the numerator,
          // then the result of the division is one.
          operator=(std::uint8_t(1U));

          if(remainder != nullptr)
          {
            *remainder = uintwide_t(std::uint8_t(0U));
          }

          return;
        }
      }

      if(v_offset == local_uint_index_type(number_of_limbs - 1U))
      {
        // The denominator has one single limb.
        // Use a one-dimensional division algorithm.
        const limb_type short_denominator = other.values[0U];

        eval_divide_by_single_limb(short_denominator, u_offset, remainder);

        return;
      }

      // We will now use the Knuth long division algorithm.
      {
        // Compute the normalization factor d.
        const double_limb_type d_large =
          double_limb_type(  ((double_limb_type(std::uint8_t(1U))) << std::numeric_limits<limb_type>::digits)
                      /   double_limb_type(double_limb_type(other.values[(number_of_limbs - 1U) - v_offset]) + limb_type(1U)));

        const limb_type d = detail::make_lo<limb_type>(d_large);

        // Step D1(b), normalize u -> u * d = uu.
        // Note the added digit in uu and also that
        // the data of uu have not been initialized yet.

        std::array<limb_type, number_of_limbs + 1U> uu;

        if(d == limb_type(1U))
        {
          // The normalization is one.
          std::copy(values.cbegin(), values.cend(), uu.begin());

          uu.back() = limb_type(0U);
        }
        else
        {
          // Multiply u by d.
          limb_type carry = 0U;

          local_uint_index_type i;

          for(i = local_uint_index_type(0U); i < local_uint_index_type(number_of_limbs - u_offset); ++i)
          {
            const double_limb_type t = double_limb_type(double_limb_type(values[i]) * d) + carry;

            uu[i] = detail::make_lo<limb_type>(t);
            carry = detail::make_hi<limb_type>(t);
          }

          uu[i] = carry;
        }

        std::array<limb_type, number_of_limbs> vv;

        // Step D1(c): normalize v -> v * d = vv.
        if(d == limb_type(1U))
        {
          // The normalization is one.
          vv = other.values;
        }
        else
        {
          // Multiply v by d.
          limb_type carry = 0U;

          for(local_uint_index_type i = local_uint_index_type(0U); i < local_uint_index_type(number_of_limbs - v_offset); ++i)
          {
            const double_limb_type t = double_limb_type(double_limb_type(other.values[i]) * d) + carry;

            vv[i] = detail::make_lo<limb_type>(t);
            carry = detail::make_hi<limb_type>(t);
          }
        }

        // Step D2: Initialize j.
        // Step D7: Loop on j from m to 0.

        const local_uint_index_type n = local_uint_index_type(number_of_limbs - v_offset);
        const local_uint_index_type m = local_uint_index_type(number_of_limbs - u_offset) - n;

        for(local_uint_index_type j = local_uint_index_type(0U); j <= m; ++j)
        {
          // Step D3 [Calculate q_hat].
          //   if u[j] == v[j0]
          //     set q_hat = b - 1
          //   else
          //     set q_hat = (u[j] * b + u[j + 1]) / v[1]

          const local_uint_index_type uj     = (((number_of_limbs + 1U) - 1U) - u_offset) - j;
          const local_uint_index_type vj0    =   (number_of_limbs       - 1U) - v_offset;
          const double_limb_type           u_j_j1 = (double_limb_type(uu[uj]) << std::numeric_limits<limb_type>::digits) + uu[uj - 1U];

          double_limb_type q_hat = ((uu[uj] == vv[vj0])
                                ? double_limb_type((std::numeric_limits<limb_type>::max)())
                                : u_j_j1 / double_limb_type(vv[vj0]));

          // Decrease q_hat if necessary.
          // This means that q_hat must be decreased if the
          // expression [(u[uj] * b + u[uj - 1] - q_hat * v[vj0 - 1]) * b]
          // exceeds the range of uintwide_t.

          double_limb_type t;

          for(;;)
          {
            t = u_j_j1 - double_limb_type(q_hat * double_limb_type(vv[vj0]));

            if(detail::make_hi<limb_type>(t) != limb_type(0U))
            {
              break;
            }

            if(   double_limb_type(double_limb_type(vv[vj0 - 1U]) * q_hat)
               <= double_limb_type((t << std::numeric_limits<limb_type>::digits) + uu[uj - 2U]))
            {
              break;
            }

            --q_hat;
          }

          // Step D4: Multiply and subtract.
          // Replace u[j, ... j + n] by u[j, ... j + n] - q_hat * v[1, ... n].

          // Set nv = q_hat * (v[1, ... n]).
          {
            std::array<limb_type, number_of_limbs + 1U> nv;

            limb_type carry = 0U;

            local_uint_index_type i;

            for(i = local_uint_index_type(0U); i < n; ++i)
            {
              t     = double_limb_type(double_limb_type(vv[i]) * q_hat) + carry;
              nv[i] = detail::make_lo<limb_type>(t);
              carry = detail::make_hi<limb_type>(t);
            }

            nv[i] = carry;

            {
              // Subtract nv[0, ... n] from u[j, ... j + n].
              std::uint_fast8_t     borrow = 0U;
              local_uint_index_type ul     = uj - n;

              for(i = local_uint_index_type(0U); i <= n; ++i, ++ul)
              {
                t      = double_limb_type(double_limb_type(uu[ul]) - nv[i]) - limb_type(borrow);
                uu[ul] =   detail::make_lo<limb_type>(t);
                borrow = ((detail::make_hi<limb_type>(t) != limb_type(0U)) ? 1U : 0U);
              }

              // Get the result data.
              values[m - j] = detail::make_lo<limb_type>(q_hat);

              // Step D5: Test the remainder.
              // Set the result value: Set result.m_data[m - j] = q_hat.
              // Use the condition (u[j] < 0), in other words if the borrow
              // is non-zero, then step D6 needs to be carried out.

              if(borrow != std::uint_fast8_t(0U))
              {
                // Step D6: Add back.
                // Add v[1, ... n] back to u[j, ... j + n],
                // and decrease the result by 1.

                carry = 0U;
                ul    = uj - n;

                for(i = local_uint_index_type(0U); i < n; ++i, ++ul)
                {
                  t      = double_limb_type(double_limb_type(uu[ul]) + vv[i]) + carry;
                  uu[ul] = detail::make_lo<limb_type>(t);
                  carry  = detail::make_hi<limb_type>(t);
                }

                // A potential test case for uint512_t is:
                //   QuotientRemainder
                //     [698937339790347543053797400564366118744312537138445607919548628175822115805812983955794321304304417541511379093392776018867245622409026835324102460829431,
                //      100041341335406267530943777943625254875702684549707174207105689918734693139781]
                //
                //     {6986485091668619828842978360442127600954041171641881730123945989288792389271,
                //      100041341335406267530943777943625254875702684549707174207105689918734693139780}

                --values[m - j];
              }
            }
          }
        }

        // Clear the data elements that have not
        // been computed in the division algorithm.
        std::fill(values.begin() + (m + 1U), values.end(), limb_type(0U));

        if(remainder != nullptr)
        {
          if(d == 1)
          {
            std::copy(uu.cbegin(),
                      uu.cbegin() + (number_of_limbs - v_offset),
                      remainder->values.begin());
          }
          else
          {
            limb_type previous_u = limb_type(0U);

            for(std::int_fast32_t rl = std::int_fast32_t(n - 1U), ul = std::int_fast32_t(number_of_limbs - (v_offset + 1U)); rl >= 0; --rl, --ul)
            {
              const double_limb_type t =
                double_limb_type(  uu[std::uint_fast32_t(ul)]
                            + double_limb_type(double_limb_type(previous_u) << std::numeric_limits<limb_type>::digits));

              remainder->values[std::uint_fast32_t(rl)] = detail::make_lo<limb_type>(double_limb_type(t / d));
              previous_u                                = limb_type(t - double_limb_type(double_limb_type(d) * remainder->values[std::uint_fast32_t(rl)]));
            }
          }

          std::fill(remainder->values.begin() + n,
                    remainder->values.end(),
                    limb_type(0U));
        }
      }
    }

    // Read string function.
    bool rd_string(const char* str_input)
    {
      std::fill(values.begin(), values.end(), limb_type(0U));

      const std::uint_fast32_t str_length = detail::strlen_unsafe(str_input);

      std::uint_fast8_t base = 10U;

      std::uint_fast32_t pos = 0U;

      // Skip over a potential plus sign.
      if((str_length > 0U) && (str_input[0U] == char('+')))
      {
        ++pos;
      }

      // Perform a dynamic detection of the base.
      if(str_length > (pos + 0U))
      {
        const bool might_be_oct_or_hex = ((str_input[pos + 0U] == char('0')) && (str_length > (pos + 1U)));

        if(might_be_oct_or_hex)
        {
          if((str_input[pos + 1U] >= char('0')) && (str_input[pos + 1U] <= char('8')))
          {
            // The input format is octal.
            base = 8U;

            pos += 1U;
          }
          else if((str_input[pos + 1U] == char('x')) || (str_input[pos + 1U] == char('X')))
          {
            // The input format is hexadecimal.
            base = 16U;

            pos += 2U;
          }
        }
        else if((str_input[pos + 0U] >= char('0')) && (str_input[pos + 0U] <= char('9')))
        {
          // The input format is decimal.
          ;
        }
      }

      bool char_is_valid = true;

      for( ; ((pos < str_length) && char_is_valid); ++pos)
      {
        std::uint8_t c = std::uint8_t(str_input[pos]);

        const bool char_is_apostrophe = (c == char(39));

        if(char_is_apostrophe == false)
        {
          if(base == 8U)
          {
            if  ((c >= char('0')) && (c <= char('8'))) { c -= std::uint8_t(0x30U); }
            else                                       { char_is_valid = false; }

            if(char_is_valid)
            {
              operator<<=(3);

              values[0U] |= std::uint8_t(c);
            }
          }
          else if(base == 10U)
          {
            if   ((c >= std::uint8_t('0')) && (c <= std::uint8_t('9'))) { c -= std::uint8_t(0x30U); }
            else                                                        { char_is_valid = false; }

            if(char_is_valid)
            {
              mul_by_limb(10U);

              operator+=(c);
            }
          }
          else if(base == 16U)
          {
            if     ((c >= std::uint8_t('a')) && (c <= std::uint8_t('f'))) { c -= std::uint8_t(  87U); }
            else if((c >= std::uint8_t('A')) && (c <= std::uint8_t('F'))) { c -= std::uint8_t(  55U); }
            else if((c >= std::uint8_t('0')) && (c <= std::uint8_t('9'))) { c -= std::uint8_t(0x30U); }
            else                                                          { char_is_valid = false; }

            if(char_is_valid)
            {
              operator<<=(4);

              values[0U] |= c;
            }
          }
        }
      }

      return char_is_valid;
    }

    void bitwise_not()
    {
      for(std::uint_fast32_t i = 0U; i < number_of_limbs; ++i)
      {
        values[i] = limb_type(~values[i]);
      }
    }

    void preincrement()
    {
      // Implement pre-increment.
      std::uint_fast32_t i = 0U;

      for( ; (i < (values.size() - 1U)) && (++values[i] == limb_type(0U)); ++i)
      {
        ;
      }

      if(i == (values.size() - 1U))
      {
        ++values[i];
      }
    }

    void predecrement()
    {
      // Implement pre-decrement.
      std::uint_fast32_t i = 0U;

      for( ; (i < (values.size() - 1U)) && (values[i]-- == limb_type(0U)); ++i)
      {
        ;
      }

      if(i == (values.size() - 1U))
      {
        --values[i];
      }
    }

    bool is_zero() const
    {
      return std::all_of(values.cbegin(),
                         values.cend(),
                         [](const limb_type& u) -> bool
                         {
                           return (u == limb_type(0U));
                         });
    }
  };

  // Define some convenient unsigned wide integer types.
  using uint64_t    = uintwide_t<   64U, std::uint16_t>;
  using uint128_t   = uintwide_t<  128U, std::uint32_t>;
  using uint256_t   = uintwide_t<  256U, std::uint32_t>;
  using uint512_t   = uintwide_t<  512U, std::uint32_t>;
  using uint1024_t  = uintwide_t< 1024U, std::uint32_t>;
  using uint2048_t  = uintwide_t< 2048U, std::uint32_t>;
  using uint4096_t  = uintwide_t< 4096U, std::uint32_t>;
  using uint8192_t  = uintwide_t< 8192U, std::uint32_t>;
  using uint16384_t = uintwide_t<16384U, std::uint32_t>;
  using uint32768_t = uintwide_t<32768U, std::uint32_t>;

  // Insert a base class for numeric_limits<> support.
  // This class inherits from std::numeric_limits<unsigned int>
  // in order to provide limits for a non-specific unsigned type.

  template<typename WideUnsignedIntegerType>
  class numeric_limits_uintwide_t_base : public std::numeric_limits<unsigned int>
  {
  private:
    using local_wide_integer_type = WideUnsignedIntegerType;

  public:
    static constexpr int digits   = static_cast<int>(local_wide_integer_type::my_digits);
    static constexpr int digits10 = static_cast<int>(local_wide_integer_type::my_digits10);

    static local_wide_integer_type (max)() { return local_wide_integer_type::limits_helper_max(); }
    static local_wide_integer_type (min)() { return local_wide_integer_type::limits_helper_min(); }
  };

  template<class T>
  struct is_integral : public std::is_integral<T> { };

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  struct is_integral<wide_integer::generic_template::uintwide_t<Digits2, LimbType>>
    : public std::integral_constant<bool, true> { };

  } } // namespace wide_integer::generic_template

  namespace std
  {
    // Specialization of std::numeric_limits<uintwide_t>.
    template<const std::uint_fast32_t Digits2,
             typename LimbType>
    class numeric_limits<wide_integer::generic_template::uintwide_t<Digits2, LimbType>>
      : public wide_integer::generic_template::numeric_limits_uintwide_t_base<wide_integer::generic_template::uintwide_t<Digits2, LimbType>> { };
  }

  namespace wide_integer { namespace generic_template {

  // Non-member binary add, sub, mul, div, mod of (uintwide_t op uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator+ (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator+=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator- (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator-=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator* (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator*=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator/ (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator/=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator% (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator%=(right); }

  // Non-member binary logic operations of (uintwide_t op uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator| (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator|=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator^ (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator^=(right); }
  template<const std::uint_fast32_t Digits2, typename LimbType> uintwide_t<Digits2, LimbType> operator& (const uintwide_t<Digits2, LimbType>& left, const uintwide_t<Digits2, LimbType>& right) { return uintwide_t<Digits2, LimbType>(left).operator&=(right); }

  // Non-member binary add, sub, mul, div, mod of (uintwide_t op IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator+(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator+=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator-(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator-=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator*(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v)
  {
    // TBD: Make separate functions for signed/unsigned IntegralType.
    // TBD: And subsequently use the optimized mul_by_limb function where appropriate.
    return uintwide_t<Digits2, LimbType>(u).operator*=(uintwide_t<Digits2, LimbType>(v));
  }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator/(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator/=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == false)), uintwide_t<Digits2, LimbType>>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator%=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == true)
                           && std::numeric_limits<IntegralType>::digits <= (std::numeric_limits<LimbType>::digits)), typename uintwide_t<Digits2, LimbType>::limb_type>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v)
  {
    uintwide_t<Digits2, LimbType> remainder;

    uintwide_t<Digits2, LimbType>(u).eval_divide_by_single_limb(v, 0U, &remainder);

    using local_limb_type = typename uintwide_t<Digits2, LimbType>::limb_type;

    return local_limb_type(remainder);
  }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)
                           && (std::is_unsigned   <IntegralType>::value == true)
                           && (std::numeric_limits<IntegralType>::digits > std::numeric_limits<LimbType>::digits)), uintwide_t<Digits2, LimbType>>::type
  operator%(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator%=(uintwide_t<Digits2, LimbType>(v)); }

  // Non-member binary add, sub, mul, div, mod of (IntegralType op uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator+(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator+=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator-(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator-=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator*(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v)
  {
    // TBD: Make separate functions for signed/unsigned IntegralType.
    // TBD: And subsequently use the optimized mul_by_limb function where appropriate.
    return uintwide_t<Digits2, LimbType>(u).operator*=(v);
  }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator/(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator/=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator%(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator%=(v); }

  // Non-member binary logic operations of (uintwide_t op IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator|(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator|=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator^(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator^=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator&(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return uintwide_t<Digits2, LimbType>(u).operator&=(uintwide_t<Digits2, LimbType>(v)); }

  // Non-member binary binary logic operations of (IntegralType op uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator|(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator|=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator^(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator^=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator&(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator&=(v); }

  // Non-member shift functions of (uintwide_t shift IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator<<(const uintwide_t<Digits2, LimbType>& u, const IntegralType n) { return uintwide_t<Digits2, LimbType>(u).operator<<=(n); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), uintwide_t<Digits2, LimbType>>::type
  operator>>(const uintwide_t<Digits2, LimbType>& u, const IntegralType n) { return uintwide_t<Digits2, LimbType>(u).operator>>=(n); }

  // Non-member comparison functions of (uintwide_t cmp uintwide_t).
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator==(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator==(v); }
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator!=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator!=(v); }
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator> (const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator> (v); }
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator< (const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator< (v); }
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator>=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator>=(v); }
  template<const std::uint_fast32_t Digits2, typename LimbType> bool operator<=(const uintwide_t<Digits2, LimbType>& u, const uintwide_t<Digits2, LimbType>& v) { return u.operator<=(v); }

  // Non-member comparison functions of (uintwide_t cmp IntegralType).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator==(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator==(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator!=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator!=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator> (const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator> (uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator< (const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator< (uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator>=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator>=(uintwide_t<Digits2, LimbType>(v)); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator<=(const uintwide_t<Digits2, LimbType>& u, const IntegralType& v) { return u.operator<=(uintwide_t<Digits2, LimbType>(v)); }

  // Non-member comparison functions of (IntegralType cmp uintwide_t).
  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator==(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator==(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator!=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator!=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator> (const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator> (v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator< (const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator< (v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator>=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator>=(v); }

  template<typename IntegralType, const std::uint_fast32_t Digits2, typename LimbType>
  typename std::enable_if<(   (std::is_fundamental<IntegralType>::value == true)
                           && (std::is_integral   <IntegralType>::value == true)), bool>::type
  operator<=(const IntegralType& u, const uintwide_t<Digits2, LimbType>& v) { return uintwide_t<Digits2, LimbType>(u).operator<=(v); }

  #if defined(WIDE_INTEGER_DISABLE_IOSTREAM)
  #else

  // I/O streaming functions.
  template<typename char_type,
           typename traits_type,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  std::basic_ostream<char_type, traits_type>&
  operator<<(std::basic_ostream<char_type, traits_type>& out,
             const uintwide_t<Digits2, LimbType>& x)
  {
    std::basic_ostringstream<char_type, traits_type> ostr;

    const std::ios::fmtflags my_flags = out.flags();

    const bool show_pos     = ((my_flags & std::ios::showpos)   == std::ios::showpos);
    const bool show_base    = ((my_flags & std::ios::showbase)  == std::ios::showbase);
    const bool is_uppercase = ((my_flags & std::ios::uppercase) == std::ios::uppercase);

    std::uint_fast8_t base_rep;

    if     ((my_flags & std::ios::oct) == std::ios::oct) { base_rep =  8U; }
    else if((my_flags & std::ios::hex) == std::ios::hex) { base_rep = 16U; }
    else                                                 { base_rep = 10U; }

    const std::uint_fast32_t field_width = std::uint_fast32_t(out.width());
    const char        fill_char   = out.fill();

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;

    if(base_rep == 8U)
    {
      char str_result[local_wide_integer_type::wr_string_max_buffer_size_oct];

      x.wr_string(str_result, base_rep, show_base, show_pos, is_uppercase, field_width, fill_char);

      static_cast<void>(ostr << str_result);
    }
    else if(base_rep == 10U)
    {
      char str_result[local_wide_integer_type::wr_string_max_buffer_size_dec];

      x.wr_string(str_result, base_rep, show_base, show_pos, is_uppercase, field_width, fill_char);

      static_cast<void>(ostr << str_result);
    }
    else if(base_rep == 16U)
    {
      char str_result[local_wide_integer_type::wr_string_max_buffer_size_hex];

      x.wr_string(str_result, base_rep, show_base, show_pos, is_uppercase, field_width, fill_char);

      static_cast<void>(ostr << str_result);
    }

    return (out << ostr.str());
  }

  template<typename char_type,
           typename traits_type,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  std::basic_istream<char_type, traits_type>&
  operator>>(std::basic_istream<char_type, traits_type>& in,
             uintwide_t<Digits2, LimbType>& x)
  {
    std::string str_in;

    in >> str_in;

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;

    x = local_wide_integer_type(str_in.c_str());

    return in;
  }

  #endif

  } } // namespace wide_integer::generic_template

  // Implement various number-theoretical tools.

  namespace wide_integer { namespace generic_template {

  namespace detail {

  template<typename UnsignedIntegralType>
  inline std::uint_fast32_t lsb_helper(const UnsignedIntegralType& x)
  {
    // Compile-time checks.
    static_assert((   (std::is_fundamental<UnsignedIntegralType>::value == true)
                   && (std::is_integral<UnsignedIntegralType>::value    == true)
                   && (std::is_unsigned<UnsignedIntegralType>::value    == true)),
                   "Error: Please check the characteristics of UnsignedIntegralType");

    std::uint_fast32_t result = 0U;

    UnsignedIntegralType mask(x);

    // This assumes that at least one bit is set.
    // Otherwise saturation of the index will occur.

    // Naive and basic LSB search.
    // TBD: This could be improved with a binary search
    // on the lowest bit position of the fundamental type.
    while((std::uint_fast32_t(mask) & 1U) == 0U)
    {
      mask >>= 1U;

      ++result;
    }

    return result;
  }

  template<typename UnsignedIntegralType>
  inline std::uint_fast32_t msb_helper(const UnsignedIntegralType& u)
  {
    // Compile-time checks.
    static_assert((   (std::is_fundamental<UnsignedIntegralType>::value == true)
                   && (std::is_integral<UnsignedIntegralType>::value    == true)
                   && (std::is_unsigned<UnsignedIntegralType>::value    == true)),
                   "Error: Please check the characteristics of UnsignedIntegralType");

    using local_unsigned_integral_type = UnsignedIntegralType;

    std::int_fast32_t i;

    // TBD: This could potentially be improved with a binary
    // search for the highest bit position in the type.

    for(i = std::int_fast32_t(std::numeric_limits<local_unsigned_integral_type>::digits - 1); i >= 0; --i)
    {
      if((u & UnsignedIntegralType(local_unsigned_integral_type(1U) << i)) != 0U)
      {
        break;
      }
    }

    return std::uint_fast32_t((std::max)(std::int_fast32_t(0), i));
  }

  template<>
  inline std::uint_fast32_t msb_helper(const std::uint32_t& u)
  {
    std::uint_fast32_t r(0);

    std::uint32_t x = u;

    // Use O(log2[N]) binary-halving in an unrolled loop to find the msb.
    if((x & UINT32_C(0xFFFF0000)) != UINT32_C(0)) { x >>= 16U; r |= UINT8_C(16); }
    if((x & UINT32_C(0x0000FF00)) != UINT32_C(0)) { x >>=  8U; r |= UINT8_C( 8); }
    if((x & UINT32_C(0x000000F0)) != UINT32_C(0)) { x >>=  4U; r |= UINT8_C( 4); }
    if((x & UINT32_C(0x0000000C)) != UINT32_C(0)) { x >>=  2U; r |= UINT8_C( 2); }
    if((x & UINT32_C(0x00000002)) != UINT32_C(0)) { x >>=  1U; r |= UINT8_C( 1); }

    return std::uint_fast32_t(r);
  }

  template<>
  inline std::uint_fast32_t msb_helper(const std::uint16_t& u)
  {
    std::uint_fast32_t r(0);

    std::uint16_t x = u;

    // Use O(log2[N]) binary-halving in an unrolled loop to find the msb.
    if((x & UINT16_C(0xFF00)) != UINT16_C(0)) { x >>= 8U; r |= UINT8_C(8); }
    if((x & UINT16_C(0x00F0)) != UINT16_C(0)) { x >>= 4U; r |= UINT8_C(4); }
    if((x & UINT16_C(0x000C)) != UINT16_C(0)) { x >>= 2U; r |= UINT8_C(2); }
    if((x & UINT16_C(0x0002)) != UINT16_C(0)) { x >>= 1U; r |= UINT8_C(1); }

    return std::uint_fast32_t(r);
  }

  template<>
  inline std::uint_fast32_t msb_helper(const std::uint8_t& u)
  {
    std::uint_fast32_t r(0);

    std::uint8_t x = u;

    // Use O(log2[N]) binary-halving in an unrolled loop to find the msb.
    if((x & UINT8_C(0xF0)) != UINT8_C(0)) { x >>= 4U; r |= UINT8_C(4); }
    if((x & UINT8_C(0x0C)) != UINT8_C(0)) { x >>= 2U; r |= UINT8_C(2); }
    if((x & UINT8_C(0x02)) != UINT8_C(0)) { x >>= 1U; r |= UINT8_C(1); }

    return std::uint_fast32_t(r);
  }

  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  void swap(uintwide_t<Digits2, LimbType>& x,
            uintwide_t<Digits2, LimbType>& y)
  {
    if(&x != &y)
    {
      using local_wide_integer_type = uintwide_t<Digits2, LimbType>;

      const local_wide_integer_type tmp_x(x);

      x = y;
      y = tmp_x;
    }
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  std::uint_fast32_t lsb(const uintwide_t<Digits2, LimbType>& x)
  {
    // Calculate the position of the least-significant bit.
    // Use a linear search starting from the least significant limbs.

    using local_wide_integer_type   = uintwide_t<Digits2, LimbType>;
    using local_const_iterator_type = typename local_wide_integer_type::const_iterator;
    using local_value_type          = typename local_wide_integer_type::limb_type;

    std::uint_fast32_t bpos = 0U;

    for(local_const_iterator_type it = x.crepresentation().cbegin(); it != x.crepresentation().cend(); ++it)
    {
      if((*it & (std::numeric_limits<local_value_type>::max)()) != 0U)
      {
        const std::uint_fast32_t offset = std::uint_fast32_t(it - x.crepresentation().cbegin());

        bpos =   detail::lsb_helper(*it)
               + std::uint_fast32_t(std::uint_fast32_t(std::numeric_limits<local_value_type>::digits) * offset);

        break;
      }
    }

    return bpos;
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  std::uint_fast32_t msb(const uintwide_t<Digits2, LimbType>& x)
  {
    // Calculate the position of the most-significant bit.
    // Use a linear search starting from the most significant limbs.

    using local_wide_integer_type           = uintwide_t<Digits2, LimbType>;
    using local_const_reverse_iterator_type = typename local_wide_integer_type::const_reverse_iterator;
    using local_value_type                  = typename local_wide_integer_type::limb_type;

    std::uint_fast32_t bpos = 0U;

    for(local_const_reverse_iterator_type ri = x.crepresentation().crbegin(); ri != x.crepresentation().crend(); ++ri)
    {
      if((*ri & (std::numeric_limits<local_value_type>::max)()) != 0U)
      {
        const std::uint_fast32_t offset = std::uint_fast32_t((x.crepresentation().crend() - 1U) - ri);

        bpos =   detail::msb_helper(*ri)
               + std::uint_fast32_t(std::uint_fast32_t(std::numeric_limits<local_value_type>::digits) * offset);

        break;
      }
    }

    return bpos;
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> sqrt(const uintwide_t<Digits2, LimbType>& m)
  {
    // Calculate the square root.

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;
    using local_limb_type         = typename local_wide_integer_type::limb_type;

    const bool argument_is_zero = std::all_of(m.crepresentation().cbegin(),
                                              m.crepresentation().cend(),
                                              [](const local_limb_type& a) -> bool
                                              {
                                                return (a == 0U);
                                              });

    local_wide_integer_type s;

    if(argument_is_zero)
    {
      s = local_wide_integer_type(std::uint_fast8_t(0U));
    }
    else
    {
      // Obtain the initial guess via algorithms
      // involving the position of the msb.
      const std::uint_fast32_t msb_pos = msb(m);

      // Obtain the initial value.
      const std::uint_fast32_t left_shift_amount =
        ((std::uint_fast32_t(msb_pos % 2U) == 0U)
          ? 1U + std::uint_fast32_t((msb_pos + 0U) / 2U)
          : 1U + std::uint_fast32_t((msb_pos + 1U) / 2U));

      local_wide_integer_type u(local_wide_integer_type(std::uint_fast8_t(1U)) << left_shift_amount);

      // Perform the iteration for the square root.
      // See Algorithm 1.13 SqrtInt, Sect. 1.5.1
      // in R.P. Brent and Paul Zimmermann, "Modern Computer Arithmetic",
      // Cambridge University Press, 2011.

      for(std::uint_fast32_t i = 0U; i < 64U; ++i)
      {
        s = u;

        u = (s + (m / s)) >> 1;

        if(u >= s)
        {
          break;
        }
      }
    }

    return s;
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> cbrt(const uintwide_t<Digits2, LimbType>& m)
  {
    // Calculate the cube root.

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;
    using local_limb_type         = typename local_wide_integer_type::limb_type;

    local_wide_integer_type s;

    const bool argument_is_zero = std::all_of(m.crepresentation().cbegin(),
                                              m.crepresentation().cend(),
                                              [](const local_limb_type& a) -> bool
                                              {
                                                return (a == 0U);
                                              });

    if(argument_is_zero)
    {
      s = local_wide_integer_type(std::uint_fast8_t(0U));
    }
    else
    {
      // Obtain the initial guess via algorithms
      // involving the position of the msb.
      const std::uint_fast32_t msb_pos = msb(m);

      // Obtain the initial value.
      const std::uint_fast32_t msb_pos_mod_3 = msb_pos % 3;

      const std::uint_fast32_t left_shift_amount =
        ((msb_pos_mod_3 == 0U)
          ? 1U + std::uint_fast32_t((msb_pos +                  0U ) / 3U)
          : 1U + std::uint_fast32_t((msb_pos + (3U - msb_pos_mod_3)) / 3U));

      local_wide_integer_type u(local_wide_integer_type(std::uint_fast8_t(1U)) << left_shift_amount);

      // Perform the iteration for the k'th root.
      // See Algorithm 1.14 RootInt, Sect. 1.5.2
      // in R.P. Brent and Paul Zimmermann, "Modern Computer Arithmetic",
      // Cambridge University Press, 2011.

      const std::uint_fast32_t three_minus_one(3U - 1U);

      for(std::uint_fast32_t i = 0U; i < 64U; ++i)
      {
        s = u;

        local_wide_integer_type m_over_s_pow_3_minus_one = m;

        for(std::uint_fast32_t j = 0U; j < 3U - 1U; ++j)
        {
          // Use a loop here to divide by s^(3 - 1) because
          // without a loop, s^(3 - 1) is likely to overflow.

          m_over_s_pow_3_minus_one /= s;
        }

        u = ((s * three_minus_one) + m_over_s_pow_3_minus_one) / 3U;

        if(u >= s)
        {
          break;
        }
      }
    }

    return s;
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> rootk(const uintwide_t<Digits2, LimbType>& m,
                                      const std::uint_fast8_t k)
  {
    // Calculate the k'th root.

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;
    using local_limb_type         = typename local_wide_integer_type::limb_type;

    local_wide_integer_type s;

    if(k < 2U)
    {
      s = m;
    }
    else if(k == 2U)
    {
      s = sqrt(m);
    }
    else
    {
      const bool argument_is_zero = std::all_of(m.crepresentation().cbegin(),
                                                m.crepresentation().cend(),
                                                [](const local_limb_type& a) -> bool
                                                {
                                                  return (a == 0U);
                                                });

      if(argument_is_zero)
      {
        s = local_wide_integer_type(std::uint_fast8_t(0U));
      }
      else
      {
        // Obtain the initial guess via algorithms
        // involving the position of the msb.
        const std::uint_fast32_t msb_pos = msb(m);

        // Obtain the initial value.
        const std::uint_fast32_t msb_pos_mod_k = msb_pos % k;

        const std::uint_fast32_t left_shift_amount =
          ((msb_pos_mod_k == 0U)
            ? 1U + std::uint_fast32_t((msb_pos +                 0U ) / k)
            : 1U + std::uint_fast32_t((msb_pos + (k - msb_pos_mod_k)) / k));

        local_wide_integer_type u(local_wide_integer_type(std::uint_fast8_t(1U)) << left_shift_amount);

        // Perform the iteration for the k'th root.
        // See Algorithm 1.14 RootInt, Sect. 1.5.2
        // in R.P. Brent and Paul Zimmermann, "Modern Computer Arithmetic",
        // Cambridge University Press, 2011.

        const std::uint_fast32_t k_minus_one(k - 1U);

        for(std::uint_fast32_t i = 0U; i < 64U; ++i)
        {
          s = u;

          local_wide_integer_type m_over_s_pow_k_minus_one = m;

          for(std::uint_fast32_t j = 0U; j < k - 1U; ++j)
          {
            // Use a loop here to divide by s^(k - 1) because
            // without a loop, s^(k - 1) is likely to overflow.

            m_over_s_pow_k_minus_one /= s;
          }

          u = ((s * k_minus_one) + m_over_s_pow_k_minus_one) / k;

          if(u >= s)
          {
            break;
          }
        }
      }
    }

    return s;
  }

  template<typename OtherUnsignedIntegralTypeP,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> pow(const uintwide_t<Digits2, LimbType>& b,
                                    const OtherUnsignedIntegralTypeP&    p)
  {
    // Calculate (b ^ p).

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;

    local_wide_integer_type result;

    const OtherUnsignedIntegralTypeP zero(std::uint8_t(0U));

    if(p == zero)
    {
      result = local_wide_integer_type(std::uint8_t(1U));
    }
    else if(p == 1U)
    {
      result = b;
    }
    else if(p == 2U)
    {
      result  = b;
      result *= b;
    }
    else
    {
      result = local_wide_integer_type(std::uint8_t(1U));

      local_wide_integer_type y      (b);
      local_wide_integer_type p_local(p);

      while(!(p_local == zero))
      {
        if(std::uint_fast8_t(p_local) & 1U)
        {
          result *= y;
        }

        y *= y;

        p_local >>= 1;
      }
    }

    return result;
  }

  template<typename OtherUnsignedIntegralTypeP,
           typename OtherUnsignedIntegralTypeM,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> powm(const uintwide_t<Digits2, LimbType>& b,
                                     const OtherUnsignedIntegralTypeP&    p,
                                     const OtherUnsignedIntegralTypeM&    m)
  {
    // Calculate (b ^ p) % m.

    using local_normal_width_type = uintwide_t<Digits2, LimbType>;
    using local_double_width_type = typename local_normal_width_type::double_width_type;
    using local_limb_type         = typename local_normal_width_type::limb_type;

          local_normal_width_type    result;
          local_double_width_type    y      (b);
    const local_double_width_type    m_local(m);

    local_limb_type p0 = static_cast<local_limb_type>(p);

    if((p0 == 0U) && (p == 0U))
    {
      result = local_normal_width_type((m != 1U) ? std::uint8_t(1U) : std::uint8_t(0U));
    }
    else if((p0 == 1U) && (p == 1U))
    {
      result = b % m;
    }
    else if((p0 == 2U) && (p == 2U))
    {
      y *= y;
      y %= m_local;

      result = local_normal_width_type(y);
    }
    else
    {
      local_double_width_type    x      (std::uint8_t(1U));
      OtherUnsignedIntegralTypeP p_local(p);

      while(!(((p0 = static_cast<local_limb_type>(p_local)) == 0U) && (p_local == 0U)))
      {
        if((p0 & 1U) != 0U)
        {
          x *= y;
          x %= m_local;
        }

        y *= y;
        y %= m_local;

        p_local >>= 1;
      }

      result = local_normal_width_type(x);
    }

    return result;
  }

  namespace detail {

  template<typename ST>
  ST integer_gcd_reduce_short(ST u, ST v)
  {
    // This implementation of GCD reduction is based on an
    // adaptation of existing code from Boost.Multiprecision.

    for(;;)
    {
      if(u > v)
      {
        std::swap(u, v);
      }

      if(u == v)
      {
        break;
      }

      v  -= u;
      v >>= detail::lsb_helper(v);
    }

    return u;
  }

  template<typename LT>
  LT integer_gcd_reduce_large(LT u, LT v)
  {
    // This implementation of GCD reduction is based on an
    // adaptation of existing code from Boost.Multiprecision.

    using local_ularge_type = LT;
    using local_ushort_type = typename detail::int_type_helper<std::uint_fast32_t(std::numeric_limits<local_ularge_type>::digits / 2)>::exact_unsigned_type;

    for(;;)
    {
      if(u > v)
      {
        std::swap(u, v);
      }

      if(u == v)
      {
        break;
      }

      if(v <= local_ularge_type((std::numeric_limits<local_ushort_type>::max)()))
      {
        u = integer_gcd_reduce_short(local_ushort_type(v),
                                     local_ushort_type(u));

        break;
      }

      v -= u;

      while((local_ushort_type(v) & 1U) == 0U)
      {
        v >>= 1;
      }
    }

    return u;
  }

  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  uintwide_t<Digits2, LimbType> gcd(const uintwide_t<Digits2, LimbType>& a,
                                    const uintwide_t<Digits2, LimbType>& b)
  {
    // This implementation of GCD is an adaptation
    // of existing code from Boost.Multiprecision.

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;
    using local_ushort_type       = typename local_wide_integer_type::limb_type;
    using local_ularge_type       = typename local_wide_integer_type::double_limb_type;

    local_wide_integer_type u(a);
    local_wide_integer_type v(b);

    local_wide_integer_type result;

    if(u == v)
    {
      // This handles cases having (u = v) and also (u = v = 0).
      result = u;
    }
    else if((static_cast<local_ushort_type>(v) == 0U) && (v == 0U))
    {
      // This handles cases having (v = 0) with (u != 0).
      result = u;
    }
    else if((static_cast<local_ushort_type>(u) == 0U) && (u == 0U))
    {
      // This handles cases having (u = 0) with (v != 0).
      result = v;
    }
    else
    {
      // Now we handle cases having (u != 0) and (v != 0).

      // Let shift := lg K, where K is the greatest
      // power of 2 dividing both u and v.

      std::uint_fast32_t left_shift_amount;

      {
        const std::uint_fast32_t u_shift = lsb(u);
        const std::uint_fast32_t v_shift = lsb(v);

        left_shift_amount = (std::min)(u_shift, v_shift);

        u >>= u_shift;
        v >>= v_shift;
      }

      for(;;)
      {
        // Now u and v are both odd, so diff(u, v) is even.
        // Let u = min(u, v), v = diff(u, v) / 2.

        if(u > v)
        {
          swap(u, v);
        }

        if(u == v)
        {
          break;
        }

        if(v <= (std::numeric_limits<local_ularge_type>::max)())
        {
          if(v <= (std::numeric_limits<local_ushort_type>::max)())
          {
            u = detail::integer_gcd_reduce_short(v.crepresentation()[0U],
                                                 u.crepresentation()[0U]);
          }
          else
          {
            const local_ularge_type v_large =
              detail::make_large(v.crepresentation()[0U],
                                 v.crepresentation()[1U]);

            const local_ularge_type u_large =
              detail::make_large(u.crepresentation()[0U],
                                 u.crepresentation()[1U]);

            u = detail::integer_gcd_reduce_large(v_large, u_large);
          }

          break;
        }

        v  -= u;
        v >>= lsb(v);
      }

      result = (u << left_shift_amount);
    }

    return result;
  }

  template<typename ST>
  typename std::enable_if<(   (std::is_fundamental<ST>::value == true)
                           && (std::is_integral   <ST>::value == true)
                           && (std::is_unsigned   <ST>::value == true)), ST>::type
  gcd(const ST& u, const ST& v)
  {
    ST result;

    if(u > v)
    {
      result = gcd(v, u);
    }
    else if(u == v)
    {
      // This handles cases having (u = v) and also (u = v = 0).
      result = u;
    }
    else if(v == 0U)
    {
      // This handles cases having (v = 0) with (u != 0).
      result = u;
    }
    else if(u == 0U)
    {
      // This handles cases having (u = 0) with (v != 0).
      result = v;
    }
    else
    {
      result = detail::integer_gcd_reduce_short(u, v);
    }

    return result;
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  class uniform_int_distribution
  {
  public:
    using result_type = uintwide_t<Digits2, LimbType>;

    struct param_type
    {
    public:
      param_type(const result_type& a = (std::numeric_limits<result_type>::min)(),
                 const result_type& b = (std::numeric_limits<result_type>::max)())
        : param_a(a),
          param_b(b) { }

      ~param_type() = default;

      param_type(const param_type& other_params) : param_a(other_params.param_a),
                                                   param_b(other_params.param_b) { }

      param_type& operator=(const param_type& other_params)
      {
        if(this != &other_params)
        {
          param_a = other_params.param_a;
          param_b = other_params.param_b;
        }

        return *this;
      }

      const result_type& get_a() const { return param_a; }
      const result_type& get_b() const { return param_b; }

      void set_a(const result_type& a) { param_a = a; }
      void set_b(const result_type& b) { param_b = b; }

    private:
      result_type param_a;
      result_type param_b;

      friend inline bool operator==(const param_type& lhs,
                                    const param_type& rhs)
      {
        return (   (lhs.param_a == rhs.param_a)
                && (lhs.param_b == rhs.param_b));
      }

      friend inline bool operator!=(const param_type& lhs,
                                    const param_type& rhs)
      {
        return (   (lhs.param_a != rhs.param_a)
                || (lhs.param_b != rhs.param_b));
      }
    };

    uniform_int_distribution() : my_params() { }

    explicit uniform_int_distribution(const result_type& a,
                                      const result_type& b = (std::numeric_limits<result_type>::max)())
        : my_params(param_type(a, b)) { }

    explicit uniform_int_distribution(const param_type& other_params)
      : my_params(other_params) { }

    uniform_int_distribution(const uniform_int_distribution& other_distribution) = delete;

    ~uniform_int_distribution() = default;

    uniform_int_distribution& operator=(const uniform_int_distribution&) = delete;

    void param(const param_type& new_params)
    {
      my_params = new_params;
    }

    const param_type& param() const { return my_params; }

    result_type a() const { return my_params.get_a(); }
    result_type b() const { return my_params.get_b(); }

    template<typename GeneratorType>
    result_type operator()(GeneratorType& generator)
    {
      return generate(generator, my_params);
    }

    template<typename GeneratorType>
    result_type operator()(GeneratorType& input_generator,
                           const param_type& input_params)
    {
      return generate(input_generator, input_params);
    }

  private:
    param_type my_params;

    template<typename GeneratorType>
    result_type generate(GeneratorType& input_generator,
                         const param_type& input_params)
    {
      // Generate random numbers r, where a <= r <= b.

      result_type result(std::uint_fast8_t(0U));

      using local_limb_type = typename result_type::limb_type;

      using generator_result_type = typename GeneratorType::result_type;

      constexpr std::uint_fast8_t digits_generator_result_type =
        std::uint_fast8_t(std::numeric_limits<generator_result_type>::digits);

      static_assert((digits_generator_result_type % 8U) == 0U,
                    "Error: Generator result type must have a multiple of 8 bits.");

      constexpr std::uint_fast8_t digits_limb_ratio =
        std::uint_fast8_t(std::numeric_limits<local_limb_type>::digits / 8U);

      constexpr std::uint_fast8_t digits_gtor_ratio =
        std::uint_fast8_t(digits_generator_result_type / 8U);

      generator_result_type value = generator_result_type();

      auto it = result.representation().begin();

      std::uint_fast32_t j = 0U;

      while(it < result.representation().end())
      {
        if((j % digits_gtor_ratio) == 0U)
        {
          value = input_generator();
        }

        const std::uint8_t next_byte = std::uint8_t(value >> ((j % digits_gtor_ratio) * 8U));

        *it |= (local_limb_type(next_byte) << ((j % digits_limb_ratio) * 8U));

        ++j;

        if((j % digits_limb_ratio) == 0U)
        {
          ++it;
        }
      }

      if(   (input_params.get_a() != (std::numeric_limits<result_type>::min)())
         || (input_params.get_b() != (std::numeric_limits<result_type>::max)()))
      {
        // r = {[input_generator() % ((b - a) + 1)] + a}

        result_type range(input_params.get_b() - input_params.get_a());
        ++range;

        result %= range;
        result += input_params.get_a();
      }

      return result;
    }
  };

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  bool operator==(const uniform_int_distribution<Digits2, LimbType>& lhs,
                  const uniform_int_distribution<Digits2, LimbType>& rhs)
  {
    return (lhs.param() == rhs.param());
  }

  template<const std::uint_fast32_t Digits2,
           typename LimbType>
  bool operator!=(const uniform_int_distribution<Digits2, LimbType>& lhs,
                  const uniform_int_distribution<Digits2, LimbType>& rhs)
  {
    return (lhs.param() != rhs.param());
  }

  template<typename DistributionType,
           typename GeneratorType,
           const std::uint_fast32_t Digits2,
           typename LimbType>
  bool miller_rabin(const uintwide_t<Digits2, LimbType>& n,
                    const std::uint_fast32_t             number_of_trials,
                    DistributionType&                    distribution,
                    GeneratorType&                       generator)
  {
    // This Miller-Rabin primality test is loosely based on
    // an adaptation of some code from Boost.Multiprecision.
    // The Boost.Multiprecision code can be found here:
    // https://www.boost.org/doc/libs/1_68_0/libs/multiprecision/doc/html/boost_multiprecision/tut/primetest.html

    // Note: Some comments in this subroutine use the Wolfram Language(TM).

    using local_wide_integer_type = uintwide_t<Digits2, LimbType>;
    using local_limb_type         = typename local_wide_integer_type::limb_type;

    {
      const local_limb_type n0(n);

      if((n0 & 1U) == 0U)
      {
        // Not prime because n is even.
        return false;
      }

      if((n0 <= 227U) && (n <= 227U))
      {
        if((n0 == 2U) && (n == 2U))
        {
          // Trivial special case of (n = 2).
          return true;
        }

        // Table[Prime[i], {i, 2, 49}] =
        // {
        //     3,   5,   7,  11,  13,  17,  19,  23,
        //    29,  31,  37,  41,  43,  47,  53,  59,
        //    61,  67,  71,  73,  79,  83,  89,  97,
        //   101, 103, 107, 109, 113, 127, 131, 137,
        //   139, 149, 151, 157, 163, 167, 173, 179,
        //   181, 191, 193, 197, 199, 211, 223, 227
        // }

        // Exclude pure small primes from 3...227.
        constexpr std::array<local_limb_type, 48U> small_primes =
        {{
          UINT8_C(  3), UINT8_C(  5), UINT8_C(  7), UINT8_C( 11), UINT8_C( 13), UINT8_C( 17), UINT8_C( 19), UINT8_C( 23),
          UINT8_C( 29), UINT8_C( 31), UINT8_C( 37), UINT8_C( 41), UINT8_C( 43), UINT8_C( 47), UINT8_C( 53), UINT8_C( 59),
          UINT8_C( 61), UINT8_C( 67), UINT8_C( 71), UINT8_C( 73), UINT8_C( 79), UINT8_C( 83), UINT8_C( 89), UINT8_C( 97),
          UINT8_C(101), UINT8_C(103), UINT8_C(107), UINT8_C(109), UINT8_C(113), UINT8_C(127), UINT8_C(131), UINT8_C(137),
          UINT8_C(139), UINT8_C(149), UINT8_C(151), UINT8_C(157), UINT8_C(163), UINT8_C(167), UINT8_C(173), UINT8_C(179),
          UINT8_C(181), UINT8_C(191), UINT8_C(193), UINT8_C(197), UINT8_C(199), UINT8_C(211), UINT8_C(223), UINT8_C(227)
        }};

        return std::binary_search(small_primes.cbegin(),
                                  small_primes.cend(),
                                  n0);
      }
    }

    // Check small factors.
    {
      // Product[Prime[i], {i, 2, 16}] = 16294579238595022365
      // Exclude small prime factors from { 3 ...  53 }.
      constexpr std::uint64_t pp0 = UINT64_C(16294579238595022365);

      const std::uint64_t m0(n % pp0);

      if(detail::integer_gcd_reduce_large(m0, pp0) != 1U)
      {
        return false;
      }
    }

    {
      // Product[Prime[i], {i, 17, 26}] = 7145393598349078859
      // Exclude small prime factors from { 59 ... 101 }.
      constexpr std::uint64_t pp1 = UINT64_C(7145393598349078859);

      const std::uint64_t m1(n % pp1);

      if(detail::integer_gcd_reduce_large(m1, pp1) != 1U)
      {
        return false;
      }
    }

    {
      // Product[Prime[i], {i, 27, 35}] = 6408001374760705163
      // Exclude small prime factors from { 103 ... 149 }.
      constexpr std::uint64_t pp2 = UINT64_C(6408001374760705163);

      const std::uint64_t m2(n % pp2);

      if(detail::integer_gcd_reduce_large(m2, pp2) != 1U)
      {
        return false;
      }
    }

    {
      // Product[Prime[i], {i, 36, 43}] = 690862709424854779
      // Exclude small prime factors from { 151 ... 191 }.
      constexpr std::uint64_t pp3 = UINT64_C(690862709424854779);

      const std::uint64_t m3(n % pp3);

      if(detail::integer_gcd_reduce_large(m3, pp3) != 1U)
      {
        return false;
      }
    }

    {
      // Product[Prime[i], {i, 44, 49}] = 80814592450549
      // Exclude small prime factors from { 193 ... 227 }.
      constexpr std::uint64_t pp4 = UINT64_C(80814592450549);

      const std::uint64_t m4(n % pp4);

      if(detail::integer_gcd_reduce_large(m4, pp4) != 1U)
      {
        return false;
      }
    }

    const local_wide_integer_type nm1(n - 1U);

    // Since we have already excluded all small factors
    // up to and including 227, n is greater than 227.

    {
      // Perform a single Fermat test which will
      // exclude many non-prime candidates.

      static const local_wide_integer_type n228(local_limb_type(228U));

      const local_wide_integer_type fn = powm(n228, nm1, n);

      const local_limb_type fn0 = static_cast<local_limb_type>(fn);

      if((fn0 != 1U) && (fn != 1U))
      {
        return false;
      }
    }

    const std::uint_fast32_t k = lsb(nm1);

    const local_wide_integer_type q = nm1 >> k;

    using local_param_type = typename DistributionType::param_type;

    const local_param_type params(local_wide_integer_type(2U), n - 2U);

    bool is_probably_prime = true;

    std::uint_fast32_t i = 0U;

    local_wide_integer_type x;
    local_wide_integer_type y;

    // Execute the random trials.
    do
    {
      x = distribution(generator, params);
      y = powm(x, q, n);

      std::uint_fast32_t j = 0U;

      while(y != nm1)
      {
        const local_limb_type y0(y);

        if((y0 == 1U) && (y == 1U))
        {
          if(j == 0U)
          {
            break;
          }
          else
          {
            is_probably_prime = false;
          }
        }
        else
        {
          ++j;

          if(j == k)
          {
            is_probably_prime = false;
          }
          else
          {
            y = powm(y, 2U, n);
          }
        }
      }

      ++i;
    }
    while((i < number_of_trials) && is_probably_prime);

    // Probably prime.
    return is_probably_prime;
  }

  } } // namespace wide_integer::generic_template

  namespace wide_integer {

  bool example001_mul_div();
  bool example001a_div_mod();
  bool example002_shl_shr();
  bool example003_sqrt();
  bool example003a_cbrt();
  bool example004_rootk_pow();
  bool example005_powm();
  bool example006_gcd();
  bool example007_random_generator();
  bool example008_miller_rabin_prime();
  bool example009_compare_mul_with_boost();
  bool example010_uint48_t();

  } // namespace wide_integer

#endif // GENERIC_TEMPLATE_UINTWIDE_T_2018_10_02_H_
