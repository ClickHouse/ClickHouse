#include <iostream>

#include <vector>

template <typename T>
std::ostream & operator<<(std::ostream & stream, const std::vector<T> & what)
{
	stream << "vector(size = " << what.size() << ", capacity = " << what.capacity() << "){";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i;
	}
	stream << "}";
	return stream;
}


#include <array>

template <typename T, size_t N>
std::ostream & operator<<(std::ostream & stream, const std::array<T, N> & what)
{
	stream << "array<" << what.size() << ">{";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i;
	}
	stream << "}";
	return stream;
}


#include <map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::map<K, V> & what)
{
	stream << "map(size = " << what.size() << "){";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i.first << ": " << i.second;
	}
	stream << "}";
	return stream;
}


#include <unordered_map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::unordered_map<K, V> & what)
{
	stream << "unordered_map(size = " << what.size() << "){";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i.first << ": " << i.second;
	}
	stream << "}";
	return stream;
}


#include <utility>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::pair<K, V> & what)
{
	stream << "pair{" << what.first << ", " << what.second << "}";
	return stream;
}

#include <ratio>

template < std::intmax_t Num,  std::intmax_t Denom>
std::ostream & operator<<(std::ostream & stream, const std::ratio<Num, Denom> & what)
{
	stream << "ratio<Num=" << Num << ", Denom=" << Denom << ">";
	return stream;
}

#include <chrono>
template <class clock, class duration>
std::ostream & operator<<(std::ostream & stream, const std::chrono::duration<clock, duration> & what)
{
  stream << "chrono::duration<clock="<<clock()<<", duration="<<duration()<<">{" << what.count() << "}";
	return stream;
}

template <class clock, class duration>
std::ostream & operator<<(std::ostream & stream, const std::chrono::time_point<clock, duration> & what)
{
	stream << "chrono::time_point{" << what.time_since_epoch() << "}";
	return stream;
}


// TODO: add more types
