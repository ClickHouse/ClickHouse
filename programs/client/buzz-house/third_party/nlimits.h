#pragma once

#include <limits>

namespace buzzhouse {

template <class T>
struct NumericLimits {
	static constexpr T Minimum() {
		return std::numeric_limits<T>::lowest();
	}
	static constexpr T Maximum() {
		return std::numeric_limits<T>::max();
	}
};

}
