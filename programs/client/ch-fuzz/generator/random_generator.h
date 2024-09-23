#pragma once

#include <cstdint>
#include <random>
#include <map>
#include <set>
#include <chrono>
#include <string>
#include <tuple>
#include <cassert>

namespace chfuzz {

static inline std::tm
make_tm(int year, int month, int day) {
	std::tm tm;
	tm.tm_year = year - 1900; // years count from 1900
	tm.tm_mon = month - 1;    // months count from January=0
	tm.tm_mday = day;         // days count from 1
	return tm;
}

const constexpr int seconds_per_day = 60*60*24;

class RandomGenerator {
private:
	uint32_t seed;

	std::uniform_int_distribution<int8_t> ints8;

	std::uniform_int_distribution<uint8_t> uints8, digits, json_cols;

	std::uniform_int_distribution<int16_t> ints16;

	std::uniform_int_distribution<uint16_t> uints16;

	std::uniform_int_distribution<int32_t> ints32;

	std::uniform_int_distribution<uint32_t> uints32, dist1, dist2, dist3, dist4, date_years,
											datetime_years, datetime64_years, months, hours, minutes;

	std::uniform_int_distribution<int64_t> ints64;

	std::uniform_int_distribution<uint64_t> uints64;

	std::uniform_real_distribution<double> doubles, zero_one;

	std::uniform_int_distribution<uint32_t> days[12] = {
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 28),
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 30),
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 30),
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 30),
	std::uniform_int_distribution<uint32_t>(1, 31),
	std::uniform_int_distribution<uint32_t>(1, 30),
	std::uniform_int_distribution<uint32_t>(1, 31)
	};

	std::vector<std::string> common_english{"is","was","are","be","have","had","were","can","said","use",
		"do","will","would","make","like","has","look","write","go","see",
		"could","been","call","am","find","did","get","come","made","may",
		"take","know","live","give","think","say","help","tell","follow","came",
		"want","show","set","put","does","must","ask","went","read","need",
		"move","try","change","play","spell","found","study","learn","should","add",
		"keep","start","thought","saw","turn","might","close","seem","open","begin",
		"got","run","walk","began","grow","took","carry","hear","stop","miss", "eat",
		"watch","let","cut","talk","being","leave", "water","day","part","sound","work",
		"place","year","back","thing","name", "sentence","man","line","boy"};

	std::vector<std::string> common_chinese{"认识你很高兴", "美国", "叫", "名字", "你们", "日本", "哪国人",
		"爸爸", "兄弟姐妹", "漂亮", "照片"};

public:
	std::mt19937 gen;

	RandomGenerator(const uint32_t in_seed) : ints8(std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()),
											  uints8(std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max()),
											  digits(static_cast<uint8_t>('0'), static_cast<uint8_t>('9')),
											  json_cols(static_cast<uint8_t>('0'), static_cast<uint8_t>('4')),
											  ints16(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()),
											  uints16(std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max()),
											  ints32(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()),
											  uints32(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max()),
											  dist1(UINT32_C(1), UINT32_C(10)), dist2(UINT32_C(1), UINT32_C(100)),
											  dist3(UINT32_C(1), UINT32_C(1000)), dist4(UINT32_C(1), UINT32_C(2)),
											  date_years(0, 2149 - 1970), datetime_years(0, 2106 - 1970), datetime64_years(0, 2299 - 1900),
											  months(1, 12), hours(0, 23), minutes(0, 59),
											  ints64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()),
											  uints64(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max()),
											  doubles(std::numeric_limits<double>::min(), std::numeric_limits<double>::max()),
											  zero_one(0, 1) {
		std::random_device rd;

		seed = in_seed ? in_seed : rd();
		gen = std::mt19937(seed);
	}

	uint32_t GetSeed() const {
		return seed;
	}

	uint32_t NextSmallNumber() {
		return dist1(gen);
	}

	uint32_t NextMediumNumber() {
		return dist2(gen);
	}

	uint32_t NextLargeNumber() {
		return dist3(gen);
	}

	uint8_t NextRandomUInt8() {
		return uints8(gen);
	}

	int8_t NextRandomInt8() {
		return ints8(gen);
	}

	uint16_t NextRandomUInt16() {
		return uints16(gen);
	}

	int16_t NextRandomInt16() {
		return ints16(gen);
	}

	uint32_t NextRandomUInt32() {
		return uints32(gen);
	}

	int32_t NextRandomInt32() {
		return ints32(gen);
	}

	uint64_t NextRandomUInt64() {
		return uints64(gen);
	}

	int64_t NextRandomInt64() {
		return ints64(gen);
	}

	char NextDigit() {
		return static_cast<char>(digits(gen));
	}

	char NextJsonCol() {
		return static_cast<char>(json_cols(gen));
	}

	double NextRandomDouble() {
		return doubles(gen);
	}

	bool NextBool() {
		return dist4(gen) == 2;
	}

	//range [1970-01-01, 2149-06-06]
	void NextDate(std::string &ret) {
		const uint32_t month = months(gen), day = days[month - 1](gen);

		ret += std::to_string(1970 + date_years(gen));
		ret += "-";
		if (month < 10) {
			ret += "0";
		}
		ret += std::to_string(month);
		ret += "-";
		if (day < 10) {
			ret += "0";
		}
		ret += std::to_string(day);
	}

	//range [1900-01-01, 2299-12-31]
	void NextDate32(std::string &ret) {
		const uint32_t month = months(gen), day = days[month - 1](gen);

		ret += std::to_string(1900 + datetime64_years(gen));
		ret += "-";
		if (month < 10) {
			ret += "0";
		}
		ret += std::to_string(month);
		ret += "-";
		if (day < 10) {
			ret += "0";
		}
		ret += std::to_string(day);
	}

	//range [1970-01-01 00:00:00, 2106-02-07 06:28:15]
	void NextDateTime(std::string &ret) {
		const uint32_t month = months(gen), day = days[month - 1](gen), hour = hours(gen),
					   minute = minutes(gen), second = minutes(gen);

		ret += std::to_string(1970 + datetime_years(gen));
		ret += "-";
		if (month < 10) {
			ret += "0";
		}
		ret += std::to_string(month);
		ret += "-";
		if (day < 10) {
			ret += "0";
		}
		ret += std::to_string(day);
		ret += " ";
		if (hour < 10) {
			ret += "0";
		}
		ret += std::to_string(hour);
		ret += ":";
		if (minute < 10) {
			ret += "0";
		}
		ret += std::to_string(minute);
		ret += ":";
		if (second < 10) {
			ret += "0";
		}
		ret += std::to_string(second);
	}

	//range [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
	void NextDateTime64(std::string &ret) {
		const uint32_t month = months(gen), day = days[month - 1](gen), hour = hours(gen),
					   minute = minutes(gen), second = minutes(gen);

		ret += std::to_string(1900 + datetime64_years(gen));
		ret += "-";
		if (month < 10) {
			ret += "0";
		}
		ret += std::to_string(month);
		ret += "-";
		if (day < 10) {
			ret += "0";
		}
		ret += std::to_string(day);
		ret += " ";
		if (hour < 10) {
			ret += "0";
		}
		ret += std::to_string(hour);
		ret += ":";
		if (minute < 10) {
			ret += "0";
		}
		ret += std::to_string(minute);
		ret += ":";
		if (second < 10) {
			ret += "0";
		}
		ret += std::to_string(second);
	}

	template <typename T>
	T ThresholdGenerator(const double always_on_prob, const double always_off_prob, T min_val, T max_val) {
		const double tmp = zero_one(gen);

		if (tmp <= always_on_prob) {
			return min_val;
		}
		if (tmp <= always_on_prob + always_off_prob) {
			return max_val;
		}
		if constexpr (std::is_same<T, uint32_t>::value) {
			std::uniform_int_distribution<uint32_t> d{min_val, max_val};
			return d(gen);
		}
		if constexpr (std::is_same<T, double>::value) {
			std::uniform_real_distribution<double> d{min_val, max_val};
			return d(gen);
		}
		assert(0);
		return 0;
	}

	double RandomGauss(const double mean, const double stddev) {
		std::normal_distribution d{mean, stddev};
		return d(gen);
	}

	double RandomZeroOne() {
		return zero_one(gen);
	}

	template <typename T>
	T RandomInt(const T min, const T max) {
		std::uniform_int_distribution<T> d(min, max);
		return d(gen);
	}

	template <typename T>
	const T& PickRandomlyFromVector(const std::vector<T> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		return vals[d(gen)];
	}

	template <typename T>
	const T& PickRandomlyFromSet(const std::set<T> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		auto it = vals.begin();
		std::advance(it, d(gen));
		return *it;
	}

	template <typename K, typename V>
	const K& PickKeyRandomlyFromMap(const std::map<K,V> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		auto it = vals.begin();
		std::advance(it, d(gen));
		return it->first;
	}

	template <typename K, typename V>
	const V& PickValueRandomlyFromMap(const std::map<K,V> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		auto it = vals.begin();
		std::advance(it, d(gen));
		return it->second;
	}

	template <typename K, typename V>
	std::tuple<K,V> PickPairRandomlyFromMap(const std::map<K,V> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		auto it = vals.begin();
		std::advance(it, d(gen));
		return std::make_tuple(it->first, it->second);
	}

	void NextString(std::string &ret, const uint32_t limit) {
		const std::string &pick = PickRandomlyFromVector(this->NextBool() ? common_english : common_chinese);

		if (pick.length() < limit) {
			ret += pick;
			return;
		}
		ret += "a";
	}
};

}
