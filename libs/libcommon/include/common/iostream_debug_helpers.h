#include <iostream>

#include <vector>

template <typename T>
std::ostream & operator<<(std::ostream & stream, std::vector<T> & what) const
{
	stream << "vector(size = " << what.size() << ", capacity = " << what.capacity() << ")[";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i;
	}
	stream << "]";
	return stream;
}


#include <array>

template <typename T, size_t N>
std::ostream & operator<<(std::ostream & stream, std::array<T, N> & what) const
{
	stream << "array(size = " << what.size() << ")[";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i;
	}
	stream << "]";
	return stream;
}


#include <map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::map<K, V> & what) const
{
	stream << "map(size = " << what.size() << ")[";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i.first << ": " << i.second;
	}
	stream << "]";
	return stream;
}


#include <unordered_map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::unordered_map<K, V> & what) const
{
	stream << "unordered_map(size = " << what.size() << ")[";
	bool first = true;
	for (const auto & i : what)
	{
		if (!first)
			stream << ", ";
		first = false;
		stream << i.first << ": " << i.second;
	}
	stream << "]";
	return stream;
}


#include <utility>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, std::pair<K, V> & what) const
{
	stream << "pair[" << what.first << ", " << what.second << "]";
	return stream;
}


// TODO: add more types
