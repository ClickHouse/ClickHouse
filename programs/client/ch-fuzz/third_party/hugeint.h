#pragma once

#include <cstdint>
#include <string>

#include "nlimits.h"

namespace chfuzz {

// Forward declaration to allow conversion between hugeint and uhugeint
struct uhugeint_t;

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	hugeint_t(int64_t value);
	constexpr hugeint_t(int64_t up, uint64_t lo) : lower(lo), upper(up) {}
	constexpr hugeint_t(const hugeint_t &rhs) = default;
	constexpr hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	void ToString(std::string &res) const;

	// comparison operators
	bool operator==(const hugeint_t &rhs) const;
	bool operator!=(const hugeint_t &rhs) const;
	bool operator<=(const hugeint_t &rhs) const;
	bool operator<(const hugeint_t &rhs) const;
	bool operator>(const hugeint_t &rhs) const;
	bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	hugeint_t operator+(const hugeint_t &rhs) const;
	hugeint_t operator-(const hugeint_t &rhs) const;
	hugeint_t operator*(const hugeint_t &rhs) const;
	hugeint_t operator/(const hugeint_t &rhs) const;
	hugeint_t operator%(const hugeint_t &rhs) const;
	hugeint_t operator-() const;

	// bitwise operators
	hugeint_t operator>>(const hugeint_t &rhs) const;
	hugeint_t operator<<(const hugeint_t &rhs) const;
	hugeint_t operator&(const hugeint_t &rhs) const;
	hugeint_t operator|(const hugeint_t &rhs) const;
	hugeint_t operator^(const hugeint_t &rhs) const;
	hugeint_t operator~() const;

	// in-place operators
	hugeint_t &operator+=(const hugeint_t &rhs);
	hugeint_t &operator-=(const hugeint_t &rhs);
	hugeint_t &operator*=(const hugeint_t &rhs);
	hugeint_t &operator/=(const hugeint_t &rhs);
	hugeint_t &operator%=(const hugeint_t &rhs);
	hugeint_t &operator>>=(const hugeint_t &rhs);
	hugeint_t &operator<<=(const hugeint_t &rhs);
	hugeint_t &operator&=(const hugeint_t &rhs);
	hugeint_t &operator|=(const hugeint_t &rhs);
	hugeint_t &operator^=(const hugeint_t &rhs);

	// boolean operators
	explicit operator bool() const;
	bool operator!() const;
};

template <>
struct NumericLimits<hugeint_t> {
	static constexpr hugeint_t Minimum() {
		return {std::numeric_limits<int64_t>::lowest(), 0};
	}
	static constexpr hugeint_t Maximum() {
		return {std::numeric_limits<int64_t>::max(), std::numeric_limits<uint64_t>::max()};
	}
};


}
