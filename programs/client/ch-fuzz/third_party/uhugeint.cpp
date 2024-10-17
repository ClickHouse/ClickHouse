#include "uhugeint.h"
#include "hugeint.h"

#include <cmath>
#include <cassert>

namespace chfuzz {

uhugeint_t::uhugeint_t(uint64_t value) {
	this->lower = value;
	this->upper = 0;
}

bool uhugeint_t::operator==(const uhugeint_t &rhs) const {
	int lower_equals = this->lower == rhs.lower;
	int upper_equals = this->upper == rhs.upper;
	return lower_equals & upper_equals;
}

bool uhugeint_t::operator!=(const uhugeint_t &rhs) const {
	int lower_not_equals = this->lower != rhs.lower;
	int upper_not_equals = this->upper != rhs.upper;
	return lower_not_equals | upper_not_equals;
}

bool uhugeint_t::operator<(const uhugeint_t &rhs) const {
	int upper_smaller = this->upper < rhs.upper;
	int upper_equal = this->upper == rhs.upper;
	int lower_smaller = this->lower < rhs.lower;
	return upper_smaller | (upper_equal & lower_smaller);
}

bool uhugeint_t::operator<=(const uhugeint_t &rhs) const {
	int upper_smaller = this->upper < rhs.upper;
	int upper_equal = this->upper == rhs.upper;
	int lower_smaller_equals = this->lower <= rhs.lower;
	return upper_smaller | (upper_equal & lower_smaller_equals);
}

bool uhugeint_t::operator>(const uhugeint_t &rhs) const {
	int upper_bigger = this->upper > rhs.upper;
	int upper_equal = this->upper == rhs.upper;
	int lower_bigger = this->lower > rhs.lower;
	return upper_bigger | (upper_equal & lower_bigger);
}

bool uhugeint_t::operator>=(const uhugeint_t &rhs) const {
	int upper_bigger = this->upper > rhs.upper;
	int upper_equal = this->upper == rhs.upper;
	int lower_bigger_equals = this->lower >= rhs.lower;
	return upper_bigger | (upper_equal & lower_bigger_equals);
}

uhugeint_t uhugeint_t::operator+(const uhugeint_t &rhs) const {
	return uhugeint_t(upper + rhs.upper + ((lower + rhs.lower) < lower), lower + rhs.lower);
}

uhugeint_t uhugeint_t::operator-(const uhugeint_t &rhs) const {
	return uhugeint_t(upper - rhs.upper - ((lower - rhs.lower) > lower), lower - rhs.lower);
}

uhugeint_t uhugeint_t::operator*(const uhugeint_t &rhs) const {
	uhugeint_t result = *this;
	result *= rhs;
	return result;
}

uhugeint_t uhugeint_t::operator>>(const uhugeint_t &rhs) const {
	const uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return uhugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		return uhugeint_t(0, upper);
	} else if (shift < 64) {
		return uhugeint_t(upper >> shift, (upper << (64 - shift)) + (lower >> shift));
	} else if ((128 > shift) && (shift > 64)) {
		return uhugeint_t(0, (upper >> (shift - 64)));
	}
	return uhugeint_t(0);
}

uhugeint_t uhugeint_t::operator<<(const uhugeint_t &rhs) const {
	const uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return uhugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		return uhugeint_t(lower, 0);
	} else if (shift < 64) {
		return uhugeint_t((upper << shift) + (lower >> (64 - shift)), lower << shift);
	} else if ((128 > shift) && (shift > 64)) {
		return uhugeint_t(lower << (shift - 64), 0);
	}
	return uhugeint_t(0);
}

uhugeint_t uhugeint_t::operator&(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower & rhs.lower;
	result.upper = upper & rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator|(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower | rhs.lower;
	result.upper = upper | rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator^(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower ^ rhs.lower;
	result.upper = upper ^ rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator~() const {
	uhugeint_t result;
	result.lower = ~lower;
	result.upper = ~upper;
	return result;
}

uhugeint_t &uhugeint_t::operator+=(const uhugeint_t &rhs) {
	*this = *this + rhs;
	return *this;
}

uhugeint_t &uhugeint_t::operator-=(const uhugeint_t &rhs) {
	*this = *this - rhs;
	return *this;
}

static uhugeint_t Multiply(uhugeint_t lhs, uhugeint_t rhs) {
	uhugeint_t result;
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	__uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
	__uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
	__uint128_t result_u128;

	result_u128 = left * right;
	result.upper = uint64_t(result_u128 >> 64);
	result.lower = uint64_t(result_u128 & 0xffffffffffffffff);
#else
	// split values into 4 32-bit parts
	uint64_t top[4] = {lhs.upper >> 32, lhs.upper & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
	uint64_t bottom[4] = {rhs.upper >> 32, rhs.upper & 0xffffffff, rhs.lower >> 32, rhs.lower & 0xffffffff};
	uint64_t products[4][4];

	// multiply each component of the values
	for (int y = 3; y > -1; y--) {
		for (int x = 3; x > -1; x--) {
			products[3 - x][y] = top[x] * bottom[y];
		}
	}

	// first row
	uint64_t fourth32 = (products[0][3] & 0xffffffff);
	uint64_t third32 = (products[0][2] & 0xffffffff) + (products[0][3] >> 32);
	uint64_t second32 = (products[0][1] & 0xffffffff) + (products[0][2] >> 32);
	uint64_t first32 = (products[0][0] & 0xffffffff) + (products[0][1] >> 32);

	// second row
	third32 += (products[1][3] & 0xffffffff);
	second32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);
	first32 += (products[1][1] & 0xffffffff) + (products[1][2] >> 32);

	// third row
	second32 += (products[2][3] & 0xffffffff);
	first32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);

	// fourth row
	first32 += (products[3][3] & 0xffffffff);

	// move carry to next digit
	third32 += fourth32 >> 32;
	second32 += third32 >> 32;
	first32 += second32 >> 32;

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32 &= 0xffffffff;
	second32 &= 0xffffffff;
	first32 &= 0xffffffff;

	// combine components
	result.lower = (third32 << 32) | fourth32;
	result.upper = (first32 << 32) | second32;
#endif
	return result;
}

uhugeint_t &uhugeint_t::operator*=(const uhugeint_t &rhs) {
	*this = Multiply(*this, rhs);
	return *this;
}

static uint8_t Bits(uhugeint_t x) {
	uint8_t out = 0;
	if (x.upper) {
		out = 64;
		for (uint64_t upper = x.upper; upper; upper >>= 1) {
			++out;
		}
	} else {
		for (uint64_t lower = x.lower; lower; lower >>= 1) {
			++out;
		}
	}
	return out;
}

static uhugeint_t DivMod(uhugeint_t lhs, uhugeint_t rhs, uhugeint_t &remainder) {
	if (rhs == 0) {
		remainder = lhs;
		return uhugeint_t(0);
	}

	remainder = uhugeint_t(0);
	if (rhs == uhugeint_t(1)) {
		return lhs;
	} else if (lhs == rhs) {
		return uhugeint_t(1);
	} else if (lhs == uhugeint_t(0) || lhs < rhs) {
		remainder = lhs;
		return uhugeint_t(0);
	}

	uhugeint_t result = 0;
	for (uint8_t idx = Bits(lhs); idx > 0; --idx) {
		result <<= 1;
		remainder <<= 1;

		if (((lhs >> (idx - 1U)) & 1) != 0) {
			remainder += 1;
		}

		if (remainder >= rhs) {
			remainder -= rhs;
			result += 1;
		}
	}
	return result;
}

static uhugeint_t Divide(uhugeint_t lhs, uhugeint_t rhs) {
	uhugeint_t remainder;
	return DivMod(lhs, rhs, remainder);
}

static uhugeint_t Modulo(uhugeint_t lhs, uhugeint_t rhs) {
	uhugeint_t remainder;
	(void)DivMod(lhs, rhs, remainder);
	return remainder;
}

uhugeint_t &uhugeint_t::operator/=(const uhugeint_t &rhs) {
	*this = Divide(*this, rhs);
	return *this;
}

uhugeint_t &uhugeint_t::operator%=(const uhugeint_t &rhs) {
	*this = Modulo(*this, rhs);
	return *this;
}

uhugeint_t uhugeint_t::operator/(const uhugeint_t &rhs) const {
	return Divide(*this, rhs);
}

uhugeint_t uhugeint_t::operator%(const uhugeint_t &rhs) const {
	return Modulo(*this, rhs);
}

static uhugeint_t NegateInPlace(const uhugeint_t &input) {
	uhugeint_t result = 0;
	result -= input;
	return result;
}

uhugeint_t uhugeint_t::operator-() const {
	return NegateInPlace(*this);
}

uhugeint_t &uhugeint_t::operator>>=(const uhugeint_t &rhs) {
	*this = *this >> rhs;
	return *this;
}

uhugeint_t &uhugeint_t::operator<<=(const uhugeint_t &rhs) {
	*this = *this << rhs;
	return *this;
}

uhugeint_t &uhugeint_t::operator&=(const uhugeint_t &rhs) {
	lower &= rhs.lower;
	upper &= rhs.upper;
	return *this;
}

uhugeint_t &uhugeint_t::operator|=(const uhugeint_t &rhs) {
	lower |= rhs.lower;
	upper |= rhs.upper;
	return *this;
}

uhugeint_t &uhugeint_t::operator^=(const uhugeint_t &rhs) {
	lower ^= rhs.lower;
	upper ^= rhs.upper;
	return *this;
}

bool uhugeint_t::operator!() const {
	return *this == 0;
}

uhugeint_t::operator bool() const {
	return *this != 0;
}

void uhugeint_t::ToString(std::string &res) const {
	std::string in;
	uhugeint_t input = *this, remainder;

	while (true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = DivMod(input, 10, remainder);
		in.insert(0, std::string(1, static_cast<char>('0' + remainder.lower)));
	}
	if (in.empty()) {
		// value is zero
		res += "0";
	} else {
		res += in;
	}
}

}
