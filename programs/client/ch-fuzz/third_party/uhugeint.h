#pragma once

#include <cstdint>
#include <string>

#include "nlimits.h"

namespace chfuzz {

// Forward declaration to allow conversion between hugeint and uhugeint
struct hugeint_t;

struct uhugeint_t {
public:
	uint64_t lower;
	uint64_t upper;

public:
	uhugeint_t() = default;
	uhugeint_t(uint64_t value);
	constexpr uhugeint_t(uint64_t up, uint64_t lo) : lower(lo), upper(up) {}
	constexpr uhugeint_t(const uhugeint_t &rhs) = default;
	constexpr uhugeint_t(uhugeint_t &&rhs) = default;
	uhugeint_t &operator=(const uhugeint_t &rhs) = default;
	uhugeint_t &operator=(uhugeint_t &&rhs) = default;

	void ToString(std::string &res) const;

	// comparison operators
	bool operator==(const uhugeint_t &rhs) const;
	bool operator!=(const uhugeint_t &rhs) const;
	bool operator<=(const uhugeint_t &rhs) const;
	bool operator<(const uhugeint_t &rhs) const;
	bool operator>(const uhugeint_t &rhs) const;
	bool operator>=(const uhugeint_t &rhs) const;

	// arithmetic operators
	uhugeint_t operator+(const uhugeint_t &rhs) const;
	uhugeint_t operator-(const uhugeint_t &rhs) const;
	uhugeint_t operator*(const uhugeint_t &rhs) const;
	uhugeint_t operator/(const uhugeint_t &rhs) const;
	uhugeint_t operator%(const uhugeint_t &rhs) const;
	uhugeint_t operator-() const;

	// bitwise operators
	uhugeint_t operator>>(const uhugeint_t &rhs) const;
	uhugeint_t operator<<(const uhugeint_t &rhs) const;
	uhugeint_t operator&(const uhugeint_t &rhs) const;
	uhugeint_t operator|(const uhugeint_t &rhs) const;
	uhugeint_t operator^(const uhugeint_t &rhs) const;
	uhugeint_t operator~() const;

	// in-place operators
	uhugeint_t &operator+=(const uhugeint_t &rhs);
	uhugeint_t &operator-=(const uhugeint_t &rhs);
	uhugeint_t &operator*=(const uhugeint_t &rhs);
	uhugeint_t &operator/=(const uhugeint_t &rhs);
	uhugeint_t &operator%=(const uhugeint_t &rhs);
	uhugeint_t &operator>>=(const uhugeint_t &rhs);
	uhugeint_t &operator<<=(const uhugeint_t &rhs);
	uhugeint_t &operator&=(const uhugeint_t &rhs);
	uhugeint_t &operator|=(const uhugeint_t &rhs);
	uhugeint_t &operator^=(const uhugeint_t &rhs);

	// boolean operators
	explicit operator bool() const;
	bool operator!() const;
};

template <>
struct NumericLimits<uhugeint_t> {
	static constexpr uhugeint_t Minimum() {
		return {0, 0};
	}
	static constexpr uhugeint_t Maximum() {
		return {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()};
	}
};

}
