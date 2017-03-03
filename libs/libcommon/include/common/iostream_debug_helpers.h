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


// TODO: add more types
