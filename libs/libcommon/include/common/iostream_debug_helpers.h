#include <iostream>

#include <vector>

template <typename T>
std::ostream & operator<<(std::ostream & stream, std::vector<T> & what)
{
	stream << "vector(" << what.size() << "/" << what.capacity() << ")[";
	size_t n = 0;
	for (auto & i : what)
	{
		if (n++)
			stream << ",";
		stream << i;
	}
	stream << "]";
	return stream;
}


#include <array>

template <typename T, size_t N>
std::ostream & operator<<(std::ostream & stream, std::array<T, N> & what)
{
	stream << "array(" << what.size() << ")[";
	size_t n = 0;
	for (auto & i : what)
	{
		if (n++)
			stream << ",";
		stream << i;
	}
	stream << "]";
	return stream;
}


#include <map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::map<K, V> & what)
{
	stream << "map(" << what.size() << ")[";
	size_t n = 0;
	for (auto & i : what)
	{
		if (n++)
			stream << ",";
		stream << i.first << ":" << i.second;
	}
	stream << "]";
	return stream;
}


#include <unordered_map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::unordered_map<K, V> & what)
{
	stream << "unordered_map(" << what.size() << ")[";
	size_t n = 0;
	for (auto & i : what)
	{
		if (n++)
			stream << ",";
		stream << i.first << ":" << i.second;
	}
	stream << "]";
	return stream;
}


#include <utility>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::pair<K, V> & what)
{
	stream << "pair[" << what.first << "," << what.second << "]";
	return stream;
}


// TODO: add more types
