/*
uinteger_t.hh
An arbitrary precision unsigned integer type for C++

Copyright (c) 2017 German Mendez Bravo (Kronuz) @ german dot mb at gmail.com
Copyright (c) 2013 - 2017 Jason Lee @ calccrypto at gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

With much help from Auston Sterling

Thanks to Stefan Deigmüller for finding
a bug in operator*.

Thanks to François Dessenne for convincing me
to do a general rewrite of this class.

Germán Mández Bravo (Kronuz) converted Jason Lee's uint128_t
to header-only and extended to arbitrary bit length.
*/

#ifndef __uint_t__
#define __uint_t__

#include <vector>
#include <string>
#include <cassert>
#include <utility>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <stdexcept>
#include <functional>
#include <type_traits>

#define ASSERT assert

// Compatibility inlines
#ifndef __has_builtin         // Optional of course
#define __has_builtin(x) 0    // Compatibility with non-clang compilers
#endif

#if defined _MSC_VER
#  define HAVE___ADDCARRY_U64
#  define HAVE___SUBBORROW_U64
#  define HAVE___ADDCARRY_U32
#  define HAVE___SUBBORROW_U32
#  define HAVE___ADDCARRY_U16
#  define HAVE___SUBBORROW_U16
#  define HAVE___UMUL128
#  define HAVE___UMUL64
#  define HAVE___UMUL32
#  include <intrin.h>
#endif

#if (defined(__clang__) && __has_builtin(__builtin_clzll)) || (defined(__GNUC__ ) && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 3)))
#  define HAVE____BUILTIN_CLZLL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_clzl)) || (defined(__GNUC__ ) && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 3)))
#  define HAVE____BUILTIN_CLZL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_clz)) || (defined(__GNUC__ ) && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 3)))
#  define HAVE____BUILTIN_CLZ
#endif
#if (defined(__clang__) && __has_builtin(__builtin_addcll))
#  define HAVE____BUILTIN_ADDCLL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_addcl))
#  define HAVE____BUILTIN_ADDCL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_addc))
#  define HAVE____BUILTIN_ADDC
#endif
#if (defined(__clang__) && __has_builtin(__builtin_subcll))
#  define HAVE____BUILTIN_SUBCLL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_subcl))
#  define HAVE____BUILTIN_SUBCL
#endif
#if (defined(__clang__) && __has_builtin(__builtin_subc))
#  define HAVE____BUILTIN_SUBC
#endif

#if defined __SIZEOF_INT128__
#define HAVE____INT128_T
#endif


#ifndef DIGIT_T
#define DIGIT_T        std::uint64_t
#endif

#ifndef HALF_DIGIT_T
#define HALF_DIGIT_T   std::uint32_t
#endif

class uinteger_t;

namespace std {  // This is probably not a good idea
	// Give uinteger_t type traits
	template <> struct is_arithmetic <uinteger_t> : std::true_type {};
	template <> struct is_integral   <uinteger_t> : std::true_type {};
	template <> struct is_unsigned   <uinteger_t> : std::true_type {};
}

class uinteger_t {
public:
	using digit = DIGIT_T;
	using half_digit = HALF_DIGIT_T;

	static constexpr std::size_t digit_octets = sizeof(digit);             // number of octets per digit
	static constexpr std::size_t digit_bits = digit_octets * 8;            // number of bits per digit
	static constexpr std::size_t half_digit_octets = sizeof(half_digit);   // number of octets per half_digit
	static constexpr std::size_t half_digit_bits = half_digit_octets * 8;  // number of bits per half_digit

	using container = std::vector<digit>;

	template <typename T>
	struct is_result {
		static const bool value = false;
	};

	template <typename T, typename Alloc>
	struct is_result<std::vector<T, Alloc>> {
		static const bool value = true;
	};

	template <typename charT, typename traits, typename Alloc>
	struct is_result<std::basic_string<charT, traits, Alloc>> {
		static const bool value = true;
	};

private:
	static_assert(digit_octets == half_digit_octets * 2, "half_digit must be exactly half the size of digit");

	static constexpr std::size_t karatsuba_cutoff = 1024 / digit_bits;
	static constexpr double growth_factor = 1.5;

	std::size_t _begin;
	std::size_t _end;
	container _value_instance;
	container& _value;
	bool _carry;

public:
	// Window to vector (uses _begin and _end)

	void reserve(std::size_t sz) {
		_value.reserve(sz + _begin);
	}

	std::size_t grow(std::size_t n) {
		// expands the vector using a growth factor
		// and returns the new capacity.
		auto cc = _value.capacity();
		if (n >= cc) {
			cc = n * growth_factor;
			_value.reserve(cc);
		}
		return cc;
	}

	void resize(std::size_t sz) {
		grow(sz + _begin);
		_value.resize(sz + _begin);
	}

	void resize(std::size_t sz, const digit& c) {
		grow(sz + _begin);
		_value.resize(sz + _begin, c);
	}

	void clear() {
		_value.clear();
		_begin = 0;
		_end = 0;
		_carry = false;
	}

	digit* data() noexcept {
		return _value.data() + _begin;
	}

	const digit* data() const noexcept {
		return _value.data() + _begin;
	}

	std::size_t size() const noexcept {
		return _end ? _end - _begin : _value.size() - _begin;
	}

	void prepend(std::size_t sz, const digit& c) {
		// Efficiently prepend by growing backwards by growth factor
		auto min = std::min(_begin, sz);
		if (min) {
			// If there is some space before `_begin`, we try using it first:
			_begin -= min;
			std::fill_n(_value.begin() + _begin, min, c);
			sz -= min;
		}
		if (sz) {
			ASSERT(_begin == 0); // _begin should be 0 in here
			// If there's still more room needed, we grow the vector:
			// Ex.: grow using prepend(3, y)
			//    sz = 3
			//    _begin = 0  (B)
			//    _end = 1  (E)
			// initially (capacity == 12):
			//              |xxxxxxxxxx- |
			//              B           E
			// after reclaiming space after `_end` (same capacity == 12):
			//              |xxxxxxxxxx  |
			//              B
			//    _end = 0
			//    csz = 10
			// grow returns the new capacity (22)
			//    isz = 12  (22 - 10)
			//    _begin = 9  (12 - 3)
			// after (capacity == (12 + 3) * 1.5 == 22):
			//    |---------yyyxxxxxxxxxx|
			//              B
			if (_end) {
				// reclaim space after `_end`
				_value.resize(_end);
				_end = 0;
			}
			auto csz = _value.size();
			auto isz = grow(csz + sz) - csz;
			_value.insert(_value.begin(), isz, c);
			_begin = isz - sz;
		}
	}

	void prepend(const digit& c) {
		prepend(1, c);
	}

	void prepend(const uinteger_t& num) {
		prepend(num.size(), 0);
		std::copy(num.begin(), num.end(), begin());
	}

	void append(std::size_t sz, const digit& c) {
		// Efficiently append by growing by growth factor
		if (_end) {
			// reclaim space after `_end`
			_value.resize(_end);
			_end = 0;
		}
		auto nsz = _value.size() + sz;
		grow(nsz);
		_value.resize(nsz, c);
	}

	void append(const digit& c) {
		append(1, c);
	}

	void append(const uinteger_t& num) {
		auto sz = num.size();
		append(sz, 0);
		std::copy(num.begin(), num.end(), end() - sz);
	}

	container::iterator begin() noexcept {
		return _value.begin() + _begin;
	}

	container::const_iterator begin() const noexcept {
		return _value.cbegin() + _begin;
	}

	container::iterator end() noexcept {
		return _end ? _value.begin() + _end : _value.end();
	}

	container::const_iterator end() const noexcept {
		return _end ? _value.cbegin() + _end : _value.cend();
	}

	container::reverse_iterator rbegin() noexcept {
		return _end ? container::reverse_iterator(_value.begin() + _end) : _value.rbegin();
	}

	container::const_reverse_iterator rbegin() const noexcept {
		return _end ? container::const_reverse_iterator(_value.cbegin() + _end) : _value.crbegin();
	}

	container::reverse_iterator rend() noexcept {
		return container::reverse_iterator(_value.begin() + _begin);
	}

	container::const_reverse_iterator rend() const noexcept {
		return container::const_reverse_iterator(_value.cbegin() + _begin);
	}

	container::reference front() {
		return *begin();
	}

	container::const_reference front() const {
		return *begin();
	}

	container::reference back() {
		return *rbegin();
	}

	container::const_reference back() const {
		return *rbegin();
	}

private:
	// Optimized primitives for operations

	static digit _bits(digit x) {
	#if defined HAVE____BUILTIN_CLZLL
		if (digit_octets == sizeof(unsigned long long)) {
			return x ? digit_bits - __builtin_clzll(x) : 1;
		}
	#endif
	#if defined HAVE____BUILTIN_CLZL
		if (digit_octets == sizeof(unsigned long)) {
			return x ? digit_bits - __builtin_clzl(x) : 1;
		}
	#endif
	#if defined HAVE____BUILTIN_CLZ
		if (digit_octets == sizeof(unsigned)) {
			return x ? digit_bits - __builtin_clz(x) : 1;
		}
	#endif
		{
			digit c = x ? 0 : 1;
			while (x) {
				x >>= 1;
				++c;
			}
			return c;
		}
	}

	static digit _mult(digit x, digit y, digit* lo) {
	#if defined HAVE___UMUL128
		if (digit_bits == 64) {
			digit h;
			digit l = _umul128(x, y, &h);  // _umul128(x, y, *hi) -> lo
			return h;
		}
	#endif
	#if defined HAVE___UMUL64
		if (digit_bits == 32) {
			digit h;
			digit l = _umul64(x, y, &h);  // _umul64(x, y, *hi) -> lo
			return h;
		}
	#endif
	#if defined HAVE___UMUL32
		if (digit_bits == 16) {
			digit h;
			digit l = _umul32(x, y, &h);  // _umul32(x, y, *hi) -> lo
			return h;
		}
	#endif
	#if defined HAVE____INT128_T
		if (digit_bits == 64) {
			auto r = static_cast<__uint128_t>(x) * static_cast<__uint128_t>(y);
			*lo = r;
			return r >> digit_bits;
		}
	#endif
		if (digit_bits == 64) {
			digit x0 = x & 0xffffffffUL;
			digit x1 = x >> 32;
			digit y0 = y & 0xffffffffUL;
			digit y1 = y >> 32;

			digit u = (x0 * y0);
			digit v = (x1 * y0) + (u >> 32);
			digit w = (x0 * y1) + (v & 0xffffffffUL);

			*lo = (w << 32) + (u & 0xffffffffUL); // low
			return (x1 * y1) + (v >> 32) + (w >> 32); // high
		} if (digit_bits == 32) {
			auto r = static_cast<std::uint64_t>(x) * static_cast<std::uint64_t>(y);
			*lo = r;
			return r >> 32;
		} if (digit_bits == 16) {
			auto r = static_cast<std::uint32_t>(x) * static_cast<std::uint32_t>(y);
			*lo = r;
			return r >> 16;
		} if (digit_bits == 8) {
			auto r = static_cast<std::uint16_t>(x) * static_cast<std::uint16_t>(y);
			*lo = r;
			return r >> 8;
		}
	}

	static digit _multadd(digit x, digit y, digit a, digit c, digit* lo) {
	#if defined HAVE___UMUL128 && defined HAVE___ADDCARRY_U64
		if (digit_bits == 64) {
			digit h;
			digit l = _umul128(x, y, &h);  // _umul128(x, y, *hi) -> lo
			return h + _addcarry_u64(c, l, a, lo);  // _addcarry_u64(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___UMUL64 && defined HAVE___ADDCARRY_U32
		if (digit_bits == 32) {
			digit h;
			digit l = _umul64(x, y, &h);  // _umul64(x, y, *hi) -> lo
			return h + _addcarry_u32(c, l, a, lo);  // _addcarry_u32(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___UMUL32 && defined HAVE___ADDCARRY_U16
		if (digit_bits == 16) {
			digit h;
			digit l = _umul32(x, y, &h);  // _umul32(x, y, *hi) -> lo
			return h + _addcarry_u16(c, l, a, lo);  // _addcarry_u16(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE____INT128_T
		if (digit_bits == 64) {
			auto r = static_cast<__uint128_t>(x) * static_cast<__uint128_t>(y) + static_cast<__uint128_t>(a) + static_cast<__uint128_t>(c);
			*lo = r;
			return r >> digit_bits;
		}
	#endif
		if (digit_bits == 64) {
			digit x0 = x & 0xffffffffUL;
			digit x1 = x >> 32;
			digit y0 = y & 0xffffffffUL;
			digit y1 = y >> 32;

			digit u = (x0 * y0) + (a & 0xffffffffUL) + (c & 0xffffffffUL);
			digit v = (x1 * y0) + (u >> 32) + (a >> 32) + (c >> 32);
			digit w = (x0 * y1) + (v & 0xffffffffUL);

			*lo = (w << 32) + (u & 0xffffffffUL); // low
			return (x1 * y1) + (v >> 32) + (w >> 32); // high
		}
		if (digit_bits == 32) {
			auto r = static_cast<std::uint64_t>(x) * static_cast<std::uint64_t>(y) + static_cast<std::uint64_t>(a) + static_cast<std::uint64_t>(c);
			*lo = r;
			return r >> 32;
		}
		if (digit_bits == 16) {
			auto r = static_cast<std::uint32_t>(x) * static_cast<std::uint32_t>(y) + static_cast<std::uint32_t>(a) + static_cast<std::uint32_t>(c);
			*lo = r;
			return r >> 16;
		}
		if (digit_bits == 8) {
			auto r = static_cast<std::uint16_t>(x) * static_cast<std::uint16_t>(y) + static_cast<std::uint16_t>(a) + static_cast<std::uint16_t>(c);
			*lo = r;
			return r >> 8;
		}
	}

	static digit _divmod(digit x_hi, digit x_lo, digit y, digit* result) {
	#if defined HAVE____INT128_T
		if (digit_bits == 64) {
			auto x = static_cast<__uint128_t>(x_hi) << digit_bits | static_cast<__uint128_t>(x_lo);
			digit q = x / y;
			digit r = x % y;

			*result = q;
			return r;
		}
	#endif
		if (digit_bits == 64) {
			// quotient
			digit q = x_lo << 1;

			// remainder
			digit r = x_hi;

			digit carry = x_lo >> 63;
			int i;

			for (i = 0; i < 64; i++) {
				auto tmp = r >> 63;
				r <<= 1;
				r |= carry;
				carry = tmp;

				if (carry == 0) {
					if (r >= y) {
						carry = 1;
					} else {
						tmp = q >> 63;
						q <<= 1;
						q |= carry;
						carry = tmp;
						continue;
					}
				}

				r -= y;
				r -= (1 - carry);
				carry = 1;
				tmp = q >> 63;
				q <<= 1;
				q |= carry;
				carry = tmp;
			}

			*result = q;
			return r;
		}
		if (digit_bits == 32) {
			auto x = static_cast<std::uint64_t>(x_hi) << 32 | static_cast<std::uint64_t>(x_lo);
			digit q = x / y;
			digit r = x % y;

			*result = q;
			return r;
		}
		if (digit_bits == 16) {
			auto x = static_cast<std::uint32_t>(x_hi) << 16 | static_cast<std::uint32_t>(x_lo);
			digit q = x / y;
			digit r = x % y;

			*result = q;
			return r;
		}
		if (digit_bits == 8) {
			auto x = static_cast<std::uint16_t>(x_hi) << 8 | static_cast<std::uint16_t>(x_lo);
			digit q = x / y;
			digit r = x % y;

			*result = q;
			return r;
		}
	}

	static digit _addcarry(digit x, digit y, digit c, digit* result) {
	#if defined HAVE___ADDCARRY_U64
		if (digit_bits == 64) {
			return _addcarry_u64(c, x, y, result);  // _addcarry_u64(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___ADDCARRY_U32
		if (digit_bits == 32) {
			return _addcarry_u32(c, x, y, result);  // _addcarry_u32(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___ADDCARRY_U16
		if (digit_bits == 16) {
			return _addcarry_u16(c, x, y, result);  // _addcarry_u16(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE____BUILTIN_ADDCLL
		if (digit_octets == sizeof(unsigned long long)) {
			unsigned long long carryout;
			*result = __builtin_addcll(x, y, c, &carryout);  // __builtin_addcll(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____BUILTIN_ADDCL
		if (digit_octets == sizeof(unsigned long)) {
			unsigned long carryout;
			*result = __builtin_addcl(x, y, c, &carryout);  // __builtin_addcl(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____BUILTIN_ADDC
		if (digit_octets == sizeof(unsigned)) {
			unsigned carryout;
			*result = __builtin_addc(x, y, c, &carryout);  // __builtin_addc(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____INT128_T
		if (digit_bits == 64) {
			auto r = static_cast<__uint128_t>(x) + static_cast<__uint128_t>(y) + static_cast<__uint128_t>(c);
			*result = r;
			return static_cast<bool>(r >> digit_bits);
		}
	#endif
		if (digit_bits == 64) {
			digit x0 = x & 0xffffffffUL;
			digit x1 = x >> 32;
			digit y0 = y & 0xffffffffUL;
			digit y1 = y >> 32;

			auto u = x0 + y0 + c;
			auto v = x1 + y1 + static_cast<bool>(u >> 32);
			*result = (v << 32) + (u & 0xffffffffUL);
			return static_cast<bool>(v >> 32);
		}
		if (digit_bits == 32) {
			auto r = static_cast<std::uint64_t>(x) + static_cast<std::uint64_t>(y) + static_cast<std::uint64_t>(c);
			*result = r;
			return static_cast<bool>(r >> 32);
		}
		if (digit_bits == 16) {
			auto r = static_cast<std::uint32_t>(x) + static_cast<std::uint32_t>(y) + static_cast<std::uint32_t>(c);
			*result = r;
			return static_cast<bool>(r >> 16);
		}
		if (digit_bits == 8) {
			auto r = static_cast<std::uint16_t>(x) + static_cast<std::uint16_t>(y) + static_cast<std::uint16_t>(c);
			*result = r;
			return static_cast<bool>(r >> 8);
		}
	}

	static digit _subborrow(digit x, digit y, digit c, digit* result) {
	#if defined HAVE___SUBBORROW_U64
		if (digit_bits == 64) {
			return _subborrow_u64(c, x, y, result);  // _subborrow_u64(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___SUBBORROW_U32
		if (digit_bits == 64) {
			return _subborrow_u32(c, x, y, result);  // _subborrow_u32(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE___SUBBORROW_U16
		if (digit_bits == 64) {
			return _subborrow_u16(c, x, y, result);  // _subborrow_u16(carryin, x, y, *sum) -> carryout
		}
	#endif
	#if defined HAVE____BUILTIN_SUBCLL
		if (digit_octets == sizeof(unsigned long long)) {
			unsigned long long carryout;
			*result = __builtin_subcll(x, y, c, &carryout);  // __builtin_subcll(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____BUILTIN_SUBCL
		if (digit_octets == sizeof(unsigned long)) {
			unsigned long carryout;
			*result = __builtin_subcl(x, y, c, &carryout);  // __builtin_subcl(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____BUILTIN_SUBC
		if (digit_octets == sizeof(unsigned)) {
			unsigned carryout;
			*result = __builtin_subc(x, y, c, &carryout);  // __builtin_subc(x, y, carryin, *carryout) -> sum
			return carryout;
		}
	#endif
	#if defined HAVE____INT128_T
		if (digit_bits == 64) {
			auto r = static_cast<__uint128_t>(x) - static_cast<__uint128_t>(y) - static_cast<__uint128_t>(c);
			*result = r;
			return static_cast<bool>(r >> 64);
		}
	#endif
		if (digit_bits == 64) {
			digit x0 = x & 0xffffffffUL;
			digit x1 = x >> 32;
			digit y0 = y & 0xffffffffUL;
			digit y1 = y >> 32;

			auto u = x0 - y0 - c;
			auto v = x1 - y1 - static_cast<bool>(u >> 32);
			*result = (v << 32) + (u & 0xffffffffUL);
			return static_cast<bool>(v >> 32);
		}
		if (digit_bits == 32) {
			auto r = static_cast<std::uint64_t>(x) - static_cast<std::uint64_t>(y) - static_cast<std::uint64_t>(c);
			*result = r;
			return static_cast<bool>(r >> 32);
		}
		if (digit_bits == 16) {
			auto r = static_cast<std::uint32_t>(x) - static_cast<std::uint32_t>(y) - static_cast<std::uint32_t>(c);
			*result = r;
			return static_cast<bool>(r >> 16);
		}
		if (digit_bits == 8) {
			auto r = static_cast<std::uint16_t>(x) - static_cast<std::uint16_t>(y) - static_cast<std::uint16_t>(c);
			*result = r;
			return static_cast<bool>(r >> 8);
		}
	}

	// Helper functions

	void trim(digit mask = 0) {
		auto rit = rbegin();
		auto rit_e = rend();

		// Masks the last value of internal vector
		mask &= (digit_bits - 1);
		if (mask && rit != rit_e) {
			*rit &= (static_cast<digit>(1) << mask) - 1;
		}

		// Removes all unused zeros from the internal vector
		auto rit_f = std::find_if(rit, rit_e, [](const digit& c) { return c; });
		resize(rit_e - rit_f); // shrink
	}

	static constexpr char chr(int ord) {
		constexpr const char _[256] = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
			'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
			'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		};
		return _[ord];
	}

	static constexpr int ord(int chr) {
		constexpr const int _[256] = {
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,

			-1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, -1, -1, -1, -1, -1,
			-1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, -1, -1, -1, -1, -1,

			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,

			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		};
		return _[chr];
	}

public:
	static constexpr unsigned base_bits(int base) {
		constexpr const unsigned _[256] = {
			0, 1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6,

			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,

			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,

			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8,
		};
		return _[base - 1];
	}

	static constexpr unsigned base_size(int base) {
		constexpr const unsigned _[256] = {
			0, 64, 41, 32, 28, 25, 23, 22, 21, 20, 19, 18, 18, 17, 17, 16,
			16, 16, 16, 15, 15, 15, 15, 14, 14, 14, 14, 14, 14, 14, 13, 13,
			13, 13, 13, 13, 13, 13, 13, 13, 12, 12, 12, 12, 12, 12, 12, 12,
			12, 12, 12, 12, 12, 12, 12, 12, 11, 11, 11, 11, 11, 11, 11, 11,

			11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
			11, 11, 11, 11, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
			10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
			10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,

			10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,

			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
			9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  8,
		};
		return _[base - 1];
	}

	static const uinteger_t uint_0() {
		static uinteger_t uint_0(0);
		return uint_0;
	}

	static const uinteger_t uint_1() {
		static uinteger_t uint_1(1);
		return uint_1;
	}

private:
	// Public Implementation
#ifdef UINT_T_PUBLIC_IMPLEMENTATION
public:
#endif
	static uinteger_t& bitwise_and(uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz > rhs_sz) {
			lhs.resize(rhs_sz); // shrink
		}

		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs.end();

		auto rhs_it = rhs.begin();

		for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it) {
			*lhs_it &= *rhs_it;
		}

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& bitwise_and(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		auto result_sz = std::max(lhs_sz, rhs_sz);
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` or `rhs` if `result` is also either `rhs` or `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		auto it = result.begin();

		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it & *rhs_it;
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++it) {
				*it = 0;
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it & *rhs_it;
			}
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = 0;
			}
		}

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t bitwise_and(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		bitwise_and(result, lhs, rhs);
		return result;
	}

	static uinteger_t& bitwise_or(uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz < rhs_sz) {
			lhs.resize(rhs_sz, 0); // grow
		}

		auto lhs_it = lhs.begin();

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs.end();

		for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it) {
			*lhs_it |= *rhs_it;
		}

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& bitwise_or(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		auto result_sz = std::max(lhs_sz, rhs_sz);
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` or `rhs` if `result` is also either `rhs` or `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		auto it = result.begin();

		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it | *rhs_it;
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++it) {
				*it = *rhs_it;
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it | *rhs_it;
			}
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = *lhs_it;
			}
		}

		// Finish up
		result.trim();
		return result;
	}
	static uinteger_t bitwise_or(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		bitwise_or(result, lhs, rhs);
		return result;
	}

	static uinteger_t& bitwise_xor(uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz < rhs_sz) {
			lhs.resize(rhs_sz, 0); // grow
		}

		auto lhs_it = lhs.begin();

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs.end();

		for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it) {
			*lhs_it ^= *rhs_it;
		}

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& bitwise_xor(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		auto result_sz = std::max(lhs_sz, rhs_sz);
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` or `rhs` if `result` is also either `rhs` or `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		auto it = result.begin();

		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it ^ *rhs_it;
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++it) {
				*it = *rhs_it;
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				*it = *lhs_it ^ *rhs_it;
			}
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = *lhs_it;
			}
		}

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t bitwise_xor(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		bitwise_xor(result, lhs, rhs);
		return result;
	}

	static uinteger_t& bitwise_inv(uinteger_t& lhs) {
		auto lhs_sz = lhs.size();

		auto b = lhs.bits();

		if (!lhs_sz) {
			lhs.append(0);
		}

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` if `result` is also `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		for (; lhs_it != lhs_it_e; ++lhs_it) {
			*lhs_it = ~*lhs_it;
		}

		// Finish up
		lhs.trim(b ? b : 1);
		return lhs;
	}

	static uinteger_t& bitwise_inv(uinteger_t& result, const uinteger_t& lhs) {
		auto lhs_sz = lhs.size();

		auto b = lhs.bits();

		auto result_sz = lhs_sz ? lhs_sz : 1;
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` if `result` is also `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto it = result.begin();
		auto it_e = it + result_sz;

		for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
			*it = ~*lhs_it;
		}
		for (; it != it_e; ++it) {
			*it = ~static_cast<digit>(0);
		}

		// Finish up
		result.trim(b ? b : 1);
		return result;
	}

	static uinteger_t bitwise_inv(const uinteger_t& lhs) {
		uinteger_t result;
		bitwise_inv(result, lhs);
		return result;
	}

	static uinteger_t& bitwise_lshift(uinteger_t& lhs, const uinteger_t& rhs) {
		if (!rhs) {
			return lhs;
		}

		uinteger_t shifts_q;
		uinteger_t shifts_r;
		auto _digit_bits = digit_bits;
		auto uint_digit_bits = uinteger_t(_digit_bits);
		divmod(shifts_q, shifts_r, rhs, uint_digit_bits);
		std::size_t shifts = static_cast<std::size_t>(shifts_q);
		std::size_t shift = static_cast<std::size_t>(shifts_r);

		if (shifts) {
			lhs.prepend(shifts, 0);
		}
		if (shift) {
			digit shifted = 0;
			auto lhs_it = lhs.begin() + shifts;
			auto lhs_it_e = lhs.end();
			for (; lhs_it != lhs_it_e; ++lhs_it) {
				auto v = (*lhs_it << shift) | shifted;
				shifted = *lhs_it >> (_digit_bits - shift);
				*lhs_it = v;
			}
			if (shifted) {
				lhs.append(shifted);
			}
		}

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& bitwise_lshift(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		if (&result._value == &lhs._value) {
			bitwise_lshift(result, rhs);
			return result;
		}
		if (!rhs) {
			result = lhs;
			return result;
		}

		auto lhs_sz = lhs.size();

		uinteger_t shifts_q;
		uinteger_t shifts_r;
		auto _digit_bits = digit_bits;
		auto uint_digit_bits = uinteger_t(_digit_bits);
		divmod(shifts_q, shifts_r, rhs, uint_digit_bits);
		std::size_t shifts = static_cast<std::size_t>(shifts_q);
		std::size_t shift = static_cast<std::size_t>(shifts_r);

		auto result_sz = lhs_sz + shifts;
		result.grow(result_sz + 1);
		result.resize(shifts, 0);
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` if `result` is also `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto it = result.begin() + shifts;

		if (shift) {
			digit shifted = 0;
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				auto v = (*lhs_it << shift) | shifted;
				shifted = *lhs_it >> (_digit_bits - shift);
				*it = v;
			}
			if (shifted) {
				result.append(shifted);
			}
		} else {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = *lhs_it;
			}
		}

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t bitwise_lshift(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		bitwise_lshift(result, lhs, rhs);
		return result;
	}

	static uinteger_t& bitwise_rshift(uinteger_t& lhs, const uinteger_t& rhs) {
		if (!rhs) {
			return lhs;
		}

		auto lhs_sz = lhs.size();

		auto _digit_bits = digit_bits;
		if (compare(rhs, uinteger_t(lhs_sz * _digit_bits)) >= 0) {
			lhs = uint_0();
			return lhs;
		}

		uinteger_t shifts_q;
		uinteger_t shifts_r;
		auto uint_digit_bits = uinteger_t(_digit_bits);
		divmod(shifts_q, shifts_r, rhs, uint_digit_bits);
		std::size_t shifts = static_cast<std::size_t>(shifts_q);
		std::size_t shift = static_cast<std::size_t>(shifts_r);

		if (shifts) {
			lhs._begin += shifts;
		}
		if (shift) {
			digit shifted = 0;
			auto lhs_rit = lhs.rbegin();
			auto lhs_rit_e = lhs.rend();
			for (; lhs_rit != lhs_rit_e; ++lhs_rit) {
				auto v = (*lhs_rit >> shift) | shifted;
				shifted = *lhs_rit << (_digit_bits - shift);
				*lhs_rit = v;
			}
			lhs.trim();
		}

		return lhs;
	}

	static uinteger_t& bitwise_rshift(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		if (&result._value == &lhs._value) {
			bitwise_lshift(result, rhs);
			return result;
		}
		if (!rhs) {
			result = lhs;
			return result;
		}

		auto lhs_sz = lhs.size();

		auto _digit_bits = digit_bits;
		if (compare(rhs, uinteger_t(lhs_sz * _digit_bits)) >= 0) {
			result = uint_0();
			return result;
		}

		uinteger_t shifts_q;
		uinteger_t shifts_r;
		auto uint_digit_bits = uinteger_t(_digit_bits);
		divmod(shifts_q, shifts_r, rhs, uint_digit_bits);
		std::size_t shifts = static_cast<std::size_t>(shifts_q);
		std::size_t shift = static_cast<std::size_t>(shifts_r);

		auto result_sz = lhs_sz - shifts;
		result.resize(result_sz);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` if `result` is also `lhs`.
		auto lhs_rit = lhs.rbegin();
		auto lhs_rit_e = lhs_rit + lhs_sz - shifts;

		auto rit = result.rbegin();
		auto rit_e = rit + result_sz;

		if (shift) {
			digit shifted = 0;
			for (; lhs_rit != lhs_rit_e; ++lhs_rit, ++rit) {
				ASSERT(rit != rit_e); (void)(rit_e);
				auto v = (*lhs_rit >> shift) | shifted;
				shifted = *lhs_rit << (_digit_bits - shift);
				*rit = v;
			}
		} else {
			for (; lhs_rit != lhs_rit_e; ++lhs_rit, ++rit) {
				ASSERT(rit != rit_e); (void)(rit_e);
				*rit = *lhs_rit;
			}
		}

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t bitwise_rshift(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		bitwise_rshift(result, lhs, rhs);
		return result;
	}

	static int compare(const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz > rhs_sz) return 1;
		if (lhs_sz < rhs_sz) return -1;

		auto lhs_rit = lhs.rbegin();
		auto lhs_rit_e = lhs.rend();

		auto rhs_rit = rhs.rbegin();

		for (; lhs_rit != lhs_rit_e && *lhs_rit == *rhs_rit; ++lhs_rit, ++rhs_rit);

		if (lhs_rit != lhs_rit_e) {
			if (*lhs_rit > *rhs_rit) return 1;
			if (*lhs_rit < *rhs_rit) return -1;
		}

		return 0;
	}

	static uinteger_t& long_add(uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz < rhs_sz) {
			lhs.reserve(rhs_sz + 1);
			lhs.resize(rhs_sz, 0); // grow
		}

		// not using `end()` because resize of `lhs.resize()` could have
		// resized `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		digit carry = 0;
		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++rhs_it, ++lhs_it) {
				carry = _addcarry(*lhs_it, *rhs_it, carry, &*lhs_it);
			}
			for (; carry && rhs_it != rhs_it_e; ++rhs_it, ++lhs_it) {
				carry = _addcarry(0, *rhs_it, carry, &*lhs_it);
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++lhs_it) {
				*lhs_it = *rhs_it;
			}
		} else {
			for (; rhs_it != rhs_it_e; ++rhs_it, ++lhs_it) {
				carry = _addcarry(*lhs_it, *rhs_it, carry, &*lhs_it);
			}
			for (; carry && lhs_it != lhs_it_e; ++lhs_it) {
				carry = _addcarry(*lhs_it, 0, carry, &*lhs_it);
			}
		}

		if (carry) {
			lhs.append(1);
		}

		lhs._carry = false;

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& long_add(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		auto result_sz = std::max(lhs_sz, rhs_sz);
		result.reserve(result_sz + 1);
		result.resize(result_sz, 0);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` or `rhs` if `result` is also either `rhs` or `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		auto it = result.begin();

		digit carry = 0;
		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				carry = _addcarry(*lhs_it, *rhs_it, carry, &*it);
			}
			for (; carry && rhs_it != rhs_it_e; ++rhs_it, ++it) {
				carry = _addcarry(0, *rhs_it, carry, &*it);
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++it) {
				*it = *rhs_it;
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				carry = _addcarry(*lhs_it, *rhs_it, carry, &*it);
			}
			for (; carry && lhs_it != lhs_it_e; ++lhs_it, ++it) {
				carry = _addcarry(*lhs_it, 0, carry, &*it);
			}
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = *lhs_it;
			}
		}

		if (carry) {
			result.append(1);
		}
		result._carry = false;

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t& add(uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!rhs) {
			return lhs;
		}
		if (!lhs) {
			lhs = rhs;
			return lhs;
		}

		return long_add(lhs, rhs);
	}

	static uinteger_t& add(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!rhs) {
			result = lhs;
			return result;
		}
		if (!lhs) {
			result = rhs;
			return result;
		}

		return long_add(result, lhs, rhs);
	}

	static uinteger_t add(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		add(result, lhs, rhs);
		return result;
	}

	static uinteger_t& long_sub(uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz < rhs_sz) {
			lhs.resize(rhs_sz, 0); // grow
		}

		// not using `end()` because resize of `lhs.resize()` could have
		// resized `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		digit borrow = 0;
		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it) {
				borrow = _subborrow(*lhs_it, *rhs_it, borrow, &*lhs_it);
			}
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it) {
				borrow = _subborrow(0, *rhs_it, borrow, &*lhs_it);
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it) {
				borrow = _subborrow(*lhs_it, *rhs_it, borrow, &*lhs_it);
			}
			for (; borrow && lhs_it != lhs_it_e; ++lhs_it) {
				borrow = _subborrow(*lhs_it, 0, borrow, &*lhs_it);
			}
		}

		lhs._carry = borrow;

		// Finish up
		lhs.trim();
		return lhs;
	}

	static uinteger_t& long_sub(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		auto result_sz = std::max(lhs_sz, rhs_sz);
		result.resize(result_sz, 0);

		// not using `end()` because resize of `result.resize()` could have
		// resized `lhs` or `rhs` if `result` is also either `rhs` or `lhs`.
		auto lhs_it = lhs.begin();
		auto lhs_it_e = lhs_it + lhs_sz;

		auto rhs_it = rhs.begin();
		auto rhs_it_e = rhs_it + rhs_sz;

		auto it = result.begin();

		digit borrow = 0;
		if (lhs_sz < rhs_sz) {
			for (; lhs_it != lhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				borrow = _subborrow(*lhs_it, *rhs_it, borrow, &*it);
			}
			for (; rhs_it != rhs_it_e; ++rhs_it, ++it) {
				borrow = _subborrow(0, *rhs_it, borrow, &*it);
			}
		} else {
			for (; rhs_it != rhs_it_e; ++lhs_it, ++rhs_it, ++it) {
				borrow = _subborrow(*lhs_it, *rhs_it, borrow, &*it);
			}
			for (; borrow && lhs_it != lhs_it_e; ++lhs_it, ++it) {
				borrow = _subborrow(*lhs_it, 0, borrow, &*it);
			}
			for (; lhs_it != lhs_it_e; ++lhs_it, ++it) {
				*it = *lhs_it;
			}
		}

		result._carry = borrow;

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t& sub(uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!rhs) {
			return lhs;
		}

		return long_sub(lhs, rhs);
	}

	static uinteger_t& sub(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!rhs) {
			result = lhs;
			return result;
		}

		return long_sub(result, lhs, rhs);
	}

	static uinteger_t sub(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		sub(result, lhs, rhs);
		return result;
	}

	// Single word long multiplication
	// Fastests, but ONLY for single sized rhs
	static uinteger_t& single_mult(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		ASSERT(rhs_sz == 1); (void)(rhs_sz);
		auto n = rhs.front();

		uinteger_t tmp;
		tmp.resize(lhs_sz + 1, 0);

		auto it_lhs = lhs.begin();
		auto it_lhs_e = lhs.end();

		auto it_result = tmp.begin();

		digit carry = 0;
		for (; it_lhs != it_lhs_e; ++it_lhs, ++it_result) {
			carry = _multadd(*it_lhs, n, 0, carry, &*it_result);
		}
		if (carry) {
			*it_result = carry;
		}

		result = std::move(tmp);

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t& long_mult(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz > rhs_sz) {
			// rhs should be the largest:
			return long_mult(result, rhs, lhs);
		}

		if (lhs_sz == 1) {
			return single_mult(result, rhs, lhs);
		}

		uinteger_t tmp;
		tmp.resize(lhs_sz + rhs_sz, 0);

		auto it_lhs = lhs.begin();
		auto it_lhs_e = lhs.end();

		auto it_rhs = rhs.begin();
		auto it_rhs_e = rhs.end();

		auto it_result = tmp.begin();
		auto it_result_s = it_result;
		auto it_result_l = it_result;

		for (; it_lhs != it_lhs_e; ++it_lhs, ++it_result) {
			if (auto lhs_it_val = *it_lhs) {
				auto _it_rhs = it_rhs;
				auto _it_result = it_result;
				digit carry = 0;
				for (; _it_rhs != it_rhs_e; ++_it_rhs, ++_it_result) {
					carry = _multadd(*_it_rhs, lhs_it_val, *_it_result, carry, &*_it_result);
				}
				if (carry) {
					*_it_result++ = carry;
				}
				if (it_result_l < _it_result) {
					it_result_l = _it_result;
				}
			}
		}

		tmp.resize(it_result_l - it_result_s); // shrink

		result = std::move(tmp);

		// Finish up
		result.trim();
		return result;
	}

	// A helper for Karatsuba multiplication to split a number in two, at n.
	static std::pair<const uinteger_t, const uinteger_t> karatsuba_mult_split(const uinteger_t& num, std::size_t n) {
		const uinteger_t a(num, num._begin, num._begin + n);
		const uinteger_t b(num, num._begin + n, num._end);
		return std::make_pair(std::move(a), std::move(b));
	}

	// If rhs has at least twice the digits of lhs, and lhs is big enough that
	// Karatsuba would pay off *if* the inputs had balanced sizes.
	// View rhs as a sequence of slices, each with lhs.size() digits,
	// and multiply the slices by lhs, one at a time.
	static uinteger_t& karatsuba_lopsided_mult(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs, std::size_t cutoff) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		ASSERT(lhs_sz > cutoff);
		ASSERT(2 * lhs_sz <= rhs_sz);

		auto rhs_begin = rhs._begin;
		std::size_t shift = 0;

		uinteger_t r;
		while (rhs_sz > 0) {
			// Multiply the next slice of rhs by lhs and add into result:
			auto slice_size = std::min(lhs_sz, rhs_sz);
			const uinteger_t rhs_slice(rhs, rhs_begin, rhs_begin + slice_size);
			uinteger_t p;
			karatsuba_mult(p, lhs, rhs_slice, cutoff);
			uinteger_t rs(r, shift, 0);
			add(rs, rs, p);
			shift += slice_size;
			rhs_sz -= slice_size;
			rhs_begin += slice_size;
		}

		result = std::move(r);
		return result;
	}

	// Karatsuba multiplication
	static uinteger_t& karatsuba_mult(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs, std::size_t cutoff = 1) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		if (lhs_sz > rhs_sz) {
			// rhs should be the largest:
			return karatsuba_mult(result, rhs, lhs, cutoff);
		}

		if (lhs_sz <= cutoff) {
			return long_mult(result, lhs, rhs);
		}

		// If a is too small compared to b, splitting on b gives a degenerate case
		// in which Karatsuba may be (even much) less efficient than long multiplication.
		if (2 * lhs_sz <= rhs_sz) {
			return karatsuba_lopsided_mult(result, lhs, rhs, cutoff);
		}

		// Karatsuba:
		//
		//                  A      B
		//               x  C      D
		//     ---------------------
		//                 AD     BD
		//       AC        BC
		//     ---------------------
		//       AC    AD + BC    BD
		//
		//  AD + BC  =
		//  AC + AD + BC + BD - AC - BD
		//  (A + B) (C + D) - AC - BD

		// Calculate the split point near the middle of the largest (rhs).
		auto shift = rhs_sz >> 1;

		// Split to get A and B:
		const auto lhs_pair = karatsuba_mult_split(lhs, shift);
		const auto& A = lhs_pair.second; // hi
		const auto& B = lhs_pair.first;  // lo

		// Split to get C and D:
		const auto rhs_pair = karatsuba_mult_split(rhs, shift);
		const auto& C = rhs_pair.second; // hi
		const auto& D = rhs_pair.first;  // lo

		// Get the pieces:
		uinteger_t AC;
		karatsuba_mult(AC, A, C, cutoff);

		uinteger_t BD;
		karatsuba_mult(BD, B, D, cutoff);
		uinteger_t AD_BC, AB, CD;
		karatsuba_mult(AD_BC, A + B, C + D, cutoff);
		AD_BC -= AC;
		AD_BC -= BD;

		// Join the pieces, AC and BD (can't overlap) into BD:
		BD.reserve(shift * 2 + AC.size());
		BD.resize(shift * 2, 0);
		BD.append(AC);

		// And add AD_BC to the middle: (AC           BD) + (    AD + BC    ):
		uinteger_t BDs(BD, shift, 0);
		add(BDs, BDs, AD_BC);

		result = std::move(BD);

		// Finish up
		result.trim();
		return result;
	}

	static uinteger_t& mult(uinteger_t& lhs, const uinteger_t& rhs) {
		// Hard to see how this could have a further optimized implementation.
		return mult(lhs, lhs, rhs);
	}

	static uinteger_t& mult(uinteger_t& result, const uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!lhs || !rhs) {
			result = uint_0();
			return result;
		}
		if (compare(lhs, uint_1()) == 0) {
			result = rhs;
			return result;
		}
		if (compare(rhs, uint_1()) == 0) {
			result = lhs;
			return result;
		}

		return karatsuba_mult(result, lhs, rhs, karatsuba_cutoff);
	}

	static uinteger_t mult(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t result;
		mult(result, lhs, rhs);
		return result;
	}

	// Single word long division
	// Fastests, but ONLY for single sized rhs
	static std::pair<std::reference_wrapper<uinteger_t>, std::reference_wrapper<uinteger_t>> single_divmod(uinteger_t& quotient, uinteger_t& remainder, const uinteger_t& lhs, const uinteger_t& rhs) {
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();

		ASSERT(rhs_sz == 1); (void)(rhs_sz);
		auto n = rhs.front();

		auto rit_lhs = lhs.rbegin();
		auto rit_lhs_e = lhs.rend();

		auto q = uint_0();
		q.resize(lhs_sz, 0);
		auto rit_q = q.rbegin();

		digit r = 0;
		for (; rit_lhs != rit_lhs_e; ++rit_lhs, ++rit_q) {
			r = _divmod(r, *rit_lhs, n, &*rit_q);
		}

		q.trim();

		quotient = std::move(q);
		remainder = r;
		return std::make_pair(std::ref(quotient), std::ref(remainder));
	}

	// Implementation of Knuth's Algorithm D
	static std::pair<std::reference_wrapper<uinteger_t>, std::reference_wrapper<uinteger_t>> knuth_divmod(uinteger_t& quotient, uinteger_t& remainder, const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t v(lhs);
		uinteger_t w(rhs);

		auto v_size = v.size();
		auto w_size = w.size();
		ASSERT(v_size >= w_size && w_size >= 2);

		// D1. normalize: shift rhs left so that its top digit is >= 63 bits.
		// shift lhs left by the same amount. Results go into w and v.
		auto d = uinteger_t(digit_bits - _bits(w.back()));
		v <<= d;
		w <<= d;

		if (*v.rbegin() >= *w.rbegin()) {
			v.append(0);
		}
		v_size = v.size();
		v.append(0);

		// Now *v.rbegin() < *w.rbegin() so quotient has at most
		// (and usually exactly) k = v.size() - w.size() digits.
		auto k = v_size - w_size;
		auto q = uint_0();
		q.resize(k + 1, 0);

		auto rit_q = q.rend() - (k + 1);

		auto it_v_b = v.begin();
		auto it_v_k = it_v_b + k;

		auto it_w = w.begin();
		auto it_w_e = w.end();

		auto rit_w = w.rbegin();
		auto wm1 = *rit_w++;
		auto wm2 = *rit_w;

		// D2. inner loop: divide v[k+0..k+n] by w[0..n]
		for (; it_v_k >= it_v_b; --it_v_k, ++rit_q) {
			// D3. Compute estimate quotient digit q; may overestimate by 1 (rare)
			digit _q;
			auto _r = _divmod(*(it_v_k + w_size), *(it_v_k + w_size - 1), wm1, &_q);
			digit mullo = 0;
			auto mulhi = _mult(_q, wm2, &mullo);
			auto rlo = *(it_v_k + w_size - 2);
			while (mulhi > _r || (mulhi == _r && mullo > rlo)) {
				--_q;
				if (_addcarry(_r, wm1, 0, &_r)) {
					break;
				}
				mulhi = _mult(_q, wm2, &mullo);
			}

			// D4. Multiply and subtract _q * w0[0:size_w] from vk[0:size_w+1]
			auto _it_v = it_v_k;
			auto _it_w = it_w;
			mulhi = 0;
			digit carry = 0;
			for (; _it_w != it_w_e; ++_it_v, ++_it_w) {
				mullo = 0;
				mulhi = _multadd(*_it_w, _q, 0, mulhi, &mullo);
				carry = _subborrow(*_it_v, mullo, carry, &*_it_v);
			}
			carry = _subborrow(*_it_v, 0, carry, &*_it_v);

			if (carry) {
				// D6. Add w back if q was too large (this branch taken rarely)
				--_q;

				_it_v = it_v_k;
				_it_w = it_w;
				carry = 0;
				for (; _it_w != it_w_e; ++_it_v, ++_it_w) {
					carry = _addcarry(*_it_v, *_it_w, carry, &*_it_v);
				}
				carry = _addcarry(*_it_v, 0, carry, &*_it_v);
			}

			/* store quotient digit */
			*rit_q = _q;
		}

		// D8. unnormalize: unshift remainder.
		v.resize(w_size);
		v >>= d;

		q.trim();
		v.trim();

		quotient = std::move(q);
		remainder = std::move(v);
		return std::make_pair(std::ref(quotient), std::ref(remainder));
	}

	static std::pair<std::reference_wrapper<uinteger_t>, std::reference_wrapper<uinteger_t>> divmod(uinteger_t& quotient, uinteger_t& remainder, const uinteger_t& lhs, const uinteger_t& rhs) {
		// First try saving some calculations:
		if (!rhs) {
			throw std::domain_error("Error: division or modulus by 0");
		}
		auto lhs_sz = lhs.size();
		auto rhs_sz = rhs.size();
		if (lhs_sz == 1 && rhs_sz == 1) {
			// Fast division and modulo for single value
			auto a = *lhs.begin();
			auto b = *rhs.begin();
			quotient = a / b;
			remainder = a % b;
			return std::make_pair(std::ref(quotient), std::ref(remainder));
		}
		if (compare(rhs, uint_1()) == 0) {
			quotient = lhs;
			remainder = uint_0();
			return std::make_pair(std::ref(quotient), std::ref(remainder));
		}
		auto compared = compare(lhs, rhs);
		if (compared == 0) {
			quotient = uint_1();
			remainder = uint_0();
			return std::make_pair(std::ref(quotient), std::ref(remainder));
		}
		if (!lhs || compared < 0) {
			quotient = uint_0();
			remainder = lhs;
			return std::make_pair(std::ref(quotient), std::ref(remainder));
		}
		if (rhs_sz == 1) {
			return single_divmod(quotient, remainder, lhs, rhs);
		}

		return knuth_divmod(quotient, remainder, lhs, rhs);
	}

	static std::pair<uinteger_t, uinteger_t> divmod(const uinteger_t& lhs, const uinteger_t& rhs) {
		uinteger_t quotient;
		uinteger_t remainder;
		divmod(quotient, remainder, lhs, rhs);
		return std::make_pair(std::move(quotient), std::move(remainder));
	}

private:
	// Constructors

	template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
	void _uint_t(const T& value) {
		append(static_cast<digit>(value));
	}

	template <typename T, typename... Args, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
	void _uint_t(const T& value, Args... args) {
		_uint_t(args...);
		append(static_cast<digit>(value));
	}

	// This constructor creates a window view of the _value
	uinteger_t(const uinteger_t& o, std::size_t begin, std::size_t end) :
		_begin(begin),
		_end(end),
		_value(o._value),
		_carry(o._carry) { }

public:
	uinteger_t() :
		_begin(0),
		_end(0),
		_value(_value_instance),
		_carry(false) { }

	uinteger_t(const uinteger_t& o) :
		_begin(0),
		_end(0),
		_value_instance(o.begin(), o.end()),
		_value(_value_instance),
		_carry(o._carry) { }

	uinteger_t(uinteger_t&& o) :
		_begin(std::move(o._begin)),
		_end(std::move(o._end)),
		_value_instance(std::move(o._value_instance)),
		_value(_value_instance),
		_carry(std::move(o._carry)) { }

	template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
	uinteger_t(const T& value) :
		_begin(0),
		_end(0),
		_value(_value_instance),
		_carry(false) {
		if (value) {
			append(static_cast<digit>(value));
		}
	}

	template <typename T, typename... Args, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
	uinteger_t(const T& value, Args... args) :
		_begin(0),
		_end(0),
		_value(_value_instance),
		_carry(false) {
		_uint_t(args...);
		append(static_cast<digit>(value));
		trim();
	}

	template <typename T, typename... Args, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
	uinteger_t(std::initializer_list<T> list) :
		_begin(0),
		_end(0),
		_value(_value_instance),
		_carry(false) {
		reserve(list.size());
		for (const auto& value : list) {
			append(static_cast<digit>(value));
		}
		trim();
	}

	template <typename T, std::size_t N>
	explicit uinteger_t(T (&s)[N], int base=10) :
		uinteger_t(s, N - 1, base) { }

	explicit uinteger_t(const unsigned char* bytes, std::size_t sz, int base) :
		uinteger_t(strtouint(bytes, sz, base)) { }

	explicit uinteger_t(const char* bytes, std::size_t sz, int base) :
		uinteger_t(strtouint(bytes, sz, base)) { }

	template <typename T>
	explicit uinteger_t(const std::vector<T>& bytes, int base=10) :
		uinteger_t(bytes.data(), bytes.size(), base) { }

	explicit uinteger_t(const std::string& bytes, int base=10) :
		uinteger_t(bytes.data(), bytes.size(), base) { }

	// Assignment Operator
	uinteger_t& operator=(const uinteger_t& o) {
		_begin = 0;
		_end = 0;
		_value = container(o.begin(), o.end());
		_carry = o._carry;
		return *this;
	}
	uinteger_t& operator=(uinteger_t&& o) {
		_begin = std::move(o._begin);
		_end = std::move(o._end);
		_value_instance = std::move(o._value_instance);
		_carry = std::move(o._carry);
		return *this;
	}

	// Typecast Operators
	explicit operator bool() const {
		return static_cast<bool>(size());
	}
	explicit operator unsigned char() const {
		return static_cast<unsigned char>(size() ? front() : 0);
	}
	explicit operator unsigned short() const {
		return static_cast<unsigned short>(size() ? front() : 0);
	}
	explicit operator unsigned int() const {
		return static_cast<unsigned int>(size() ? front() : 0);
	}
	explicit operator unsigned long() const {
		return static_cast<unsigned long>(size() ? front() : 0);
	}
	explicit operator unsigned long long() const {
		return static_cast<unsigned long long>(size() ? front() : 0);
	}
	explicit operator char() const {
		return static_cast<char>(size() ? front() : 0);
	}
	explicit operator short() const {
		return static_cast<short>(size() ? front() : 0);
	}
	explicit operator int() const {
		return static_cast<int>(size() ? front() : 0);
	}
	explicit operator long() const {
		return static_cast<long>(size() ? front() : 0);
	}
	explicit operator long long() const {
		return static_cast<long long>(size() ? front() : 0);
	}

	// Bitwise Operators
	uinteger_t operator&(const uinteger_t& rhs) const {
		return bitwise_and(*this, rhs);
	}

	uinteger_t& operator&=(const uinteger_t& rhs) {
		return bitwise_and(*this, rhs);
	}

	uinteger_t operator|(const uinteger_t& rhs) const {
		return bitwise_or(*this, rhs);
	}

	uinteger_t& operator|=(const uinteger_t& rhs) {
		return bitwise_or(*this, rhs);
	}

	uinteger_t operator^(const uinteger_t& rhs) const {
		return bitwise_xor(*this, rhs);
	}

	uinteger_t& operator^=(const uinteger_t& rhs) {
		return bitwise_xor(*this, rhs);
	}

	uinteger_t operator~() const {
		return bitwise_inv(*this);
	}

	uinteger_t inv() {
		return bitwise_inv(*this);
	}

	// Bit Shift Operators
	uinteger_t operator<<(const uinteger_t& rhs) const {
		return bitwise_lshift(*this, rhs);
	}

	uinteger_t& operator<<=(const uinteger_t& rhs) {
		return bitwise_lshift(*this, rhs);
	}

	uinteger_t operator>>(const uinteger_t& rhs) const {
		return bitwise_rshift(*this, rhs);
	}

	uinteger_t& operator>>=(const uinteger_t& rhs) {
		return bitwise_rshift(*this, rhs);
	}

	// Logical Operators
	bool operator!() const {
		return !static_cast<bool>(*this);
	}

	bool operator&&(const uinteger_t& rhs) const {
		return static_cast<bool>(*this) && rhs;
	}

	bool operator||(const uinteger_t& rhs) const {
		return static_cast<bool>(*this) || rhs;
	}

	// Comparison Operators
	bool operator==(const uinteger_t& rhs) const {
		return compare(*this, rhs) == 0;
	}

	bool operator!=(const uinteger_t& rhs) const {
		return compare(*this, rhs) != 0;
	}

	bool operator>(const uinteger_t& rhs) const {
		return compare(*this, rhs) > 0;
	}

	bool operator<(const uinteger_t& rhs) const {
		return compare(*this, rhs) < 0;
	}

	bool operator>=(const uinteger_t& rhs) const {
		return compare(*this, rhs) >= 0;
	}

	bool operator<=(const uinteger_t& rhs) const {
		return compare(*this, rhs) <= 0;
	}

	// Arithmetic Operators
	uinteger_t operator+(const uinteger_t& rhs) const {
		return add(*this, rhs);
	}

	uinteger_t& operator+=(const uinteger_t& rhs) {
		return add(*this, rhs);
	}

	uinteger_t operator-(const uinteger_t& rhs) const {
		return sub(*this, rhs);
	}

	uinteger_t& operator-=(const uinteger_t& rhs) {
		return sub(*this, rhs);
	}

	uinteger_t operator*(const uinteger_t& rhs) const {
		return mult(*this, rhs);
	}

	uinteger_t& operator*=(const uinteger_t& rhs) {
		return mult(*this, rhs);
	}

	std::pair<uinteger_t, uinteger_t> divmod(const uinteger_t& rhs) const {
		return divmod(*this, rhs);
	}

	uinteger_t operator/(const uinteger_t& rhs) const {
		return divmod(*this, rhs).first;
	}

	uinteger_t& operator/=(const uinteger_t& rhs) {
		uinteger_t quotient;
		uinteger_t remainder;
		divmod(quotient, remainder, *this, rhs);
		*this = std::move(quotient);
		return *this;
	}

	uinteger_t operator%(const uinteger_t& rhs) const {
		return divmod(*this, rhs).second;
	}

	uinteger_t& operator%=(const uinteger_t& rhs) {
		uinteger_t quotient;
		uinteger_t remainder;
		divmod(quotient, remainder, *this, rhs);
		*this = std::move(remainder);
		return *this;
	}

	// Increment Operator
	uinteger_t& operator++() {
		return *this += uint_1();
	}
	uinteger_t operator++(int) {
		uinteger_t temp(*this);
		++*this;
		return temp;
	}

	// Decrement Operator
	uinteger_t& operator--() {
		return *this -= uint_1();
	}
	uinteger_t operator--(int) {
		uinteger_t temp(*this);
		--*this;
		return temp;
	}

	// Nothing done since promotion doesn't work here
	uinteger_t operator+() const {
		return *this;
	}

	// two's complement
	uinteger_t operator-() const {
		return uint_0() - *this;
	}

	// Get private value at index
	const digit& value(std::size_t idx) const {
		static const digit zero = 0;
		return idx < size() ? *(begin() + idx) : zero;
	}

	// Get value of bit N
	bool operator[](std::size_t n) const {
		auto nd = n / digit_bits;
		auto nm = n % digit_bits;
		return nd < size() ? (*(begin() + nd) >> nm) & 1 : 0;
	}

	// Get bitsize of value
	std::size_t bits() const {
		auto sz = size();
		if (sz) {
			return _bits(back()) + (sz - 1) * digit_bits;
		}
		return 0;
	}

	// Get string representation of value
	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result str(int alphabet_base = 10) const {
		auto num_sz = size();
		if (alphabet_base >= 2 && alphabet_base <= 36) {
			Result result;
			if (num_sz) {
				auto alphabet_base_bits = base_bits(alphabet_base);
				result.reserve(num_sz * base_size(alphabet_base));
				if (alphabet_base_bits) {
					digit alphabet_base_mask = alphabet_base - 1;
					std::size_t shift = 0;
					auto ptr = reinterpret_cast<const half_digit*>(data());
					digit v = *ptr++;
					v <<= half_digit_bits;
					for (auto i = num_sz * 2 - 1; i; --i) {
						v >>= half_digit_bits;
						v |= (static_cast<digit>(*ptr++) << half_digit_bits);
						do {
							auto d = static_cast<int>((v >> shift) & alphabet_base_mask);
							result.push_back(chr(d));
							shift += alphabet_base_bits;
						} while (shift <= half_digit_bits);
						shift -= half_digit_bits;
					}
					v >>= (shift + half_digit_bits);
					while (v) {
						auto d = static_cast<int>(v & alphabet_base_mask);
						result.push_back(chr(d));
						v >>= alphabet_base_bits;
					}
					auto s = chr(0);
					auto rit_f = std::find_if(result.rbegin(), result.rend(), [s](const char& c) { return c != s; });
					result.resize(result.rend() - rit_f); // shrink
				} else {
					uinteger_t uint_base = alphabet_base;
					uinteger_t quotient = *this;
					do {
						auto r = quotient.divmod(uint_base);
						auto d = static_cast<int>(r.second);
						result.push_back(chr(d));
						quotient = std::move(r.first);
					} while (quotient);
				}
				std::reverse(result.begin(), result.end());
			} else {
				result.push_back(chr(0));
			}
			return result;
		} else if (alphabet_base == 256) {
			if (num_sz) {
				auto ptr = reinterpret_cast<const char*>(data());
				Result result(ptr, ptr + num_sz * digit_octets);
				auto rit_f = std::find_if(result.rbegin(), result.rend(), [](const char& c) { return c; });
				result.resize(result.rend() - rit_f); // shrink
				std::reverse(result.begin(), result.end());
				return result;
			} else {
				Result result;
				result.push_back('\x00');
				return result;
			}
		} else {
			throw std::invalid_argument("Base must be in the range [2, 36]");
		}
	}

	static uinteger_t strtouint(const void* encoded, std::size_t encoded_size, int alphabet_base) {
		const char* data = (const char *)encoded;
		uinteger_t result;

		if (alphabet_base >= 2 && alphabet_base <= 36) {
			uinteger_t alphabet_base_bits = base_bits(alphabet_base);
			uinteger_t uint_base = alphabet_base;
			if (alphabet_base_bits) {
				for (; encoded_size; --encoded_size, ++data) {
					auto d = ord(static_cast<int>(*data));
					if (d < 0) {
						throw std::invalid_argument("Error: Not a digit in base " + std::to_string(alphabet_base) + ": '" + std::string(1, *data) + "' at " + std::to_string(encoded_size));
					}
					result = (result << alphabet_base_bits) | d;
				}
			} else {
				for (; encoded_size; --encoded_size, ++data) {
					auto d = ord(static_cast<int>(*data));
					if (d < 0) {
						throw std::invalid_argument("Error: Not a digit in base " + std::to_string(alphabet_base) + ": '" + std::string(1, *data) + "' at " + std::to_string(encoded_size));
					}
					result = (result * uint_base) + d;
				}
			}
		} else if (encoded_size && alphabet_base == 256) {
			auto value_size = encoded_size / digit_octets;
			auto value_padding = encoded_size % digit_octets;
			if (value_padding) {
				value_padding = digit_octets - value_padding;
				++value_size;
			}
			result.resize(value_size); // grow (no initialization)
			*result.begin() = 0; // initialize value
			auto ptr = reinterpret_cast<char*>(result.data());
			std::copy(data, data + encoded_size, ptr + value_padding);
			std::reverse(ptr, ptr + value_size * digit_octets);
		} else {
			throw std::invalid_argument("Error: Cannot convert from base " + std::to_string(alphabet_base));
		}

		return result;
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result bin() const {
		return str<Result>(2);
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result oct() const {
		return str<Result>(8);
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result hex() const {
		return str<Result>(16);
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result raw() const {
		return str<Result>(256);
	}
};

namespace std {  // This is probably not a good idea
	// Make it work with std::string()
	inline std::string to_string(uinteger_t& num) {
		return num.str();
	}
	inline const std::string to_string(const uinteger_t& num) {
		return num.str();
	}
}

// lhs type T as first arguemnt
// If the output is not a bool, casts to type T

// Bitwise Operators
template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator&(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) & rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator&=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(rhs & lhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator|(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) | rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator|=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(rhs | lhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator^(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) ^ rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator^=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(rhs ^ lhs);
}

// Bitshift operators
template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
inline uinteger_t operator<<(T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) << rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator<<=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(lhs << rhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
inline uinteger_t operator>>(T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) >> rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator>>=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(lhs >> rhs);
}

// Comparison Operators
template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator==(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) == rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator!=(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) != rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator>(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) > rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator<(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) < rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator>=(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) >= rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
bool operator<=(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) <= rhs;
}

// Arithmetic Operators
template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator+(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) + rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator+=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(rhs + lhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator-(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) - rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator-=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(lhs - rhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator*(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) * rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator*=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(rhs * lhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator/(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) / rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator/=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(lhs / rhs);
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
uinteger_t operator%(const T& lhs, const uinteger_t& rhs) {
	return uinteger_t(lhs) % rhs;
}

template <typename T, typename = typename std::enable_if_t<std::is_integral<T>::value and not std::is_same<T, std::decay_t<uinteger_t>>::value>>
T& operator%=(T& lhs, const uinteger_t& rhs) {
	return lhs = static_cast<T>(lhs % rhs);
}

// IO Operator
inline std::ostream& operator<<(std::ostream& stream, const uinteger_t& rhs) {
	if (stream.flags() & stream.oct) {
		stream << rhs.str(8);
	} else if (stream.flags() & stream.dec) {
		stream << rhs.str(10);
	} else if (stream.flags() & stream.hex) {
		stream << rhs.str(16);
	}
	return stream;
}

#endif
