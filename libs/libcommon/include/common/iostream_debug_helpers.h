#pragma once
#include <iostream>


#include <utility>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::pair<K, V> & what)
{
    stream << "pair{" << what.first << ", " << what.second << "}";
    return stream;
}


template <typename T>
void dumpContainer(std::ostream & stream, const T & container)
{
    stream << "{";
    bool first = true;
    for (const auto & elem : container)
    {
        if (!first)
            stream << ", ";
        first = false;
        stream << elem;
    }
    stream << "}";
}


#include <vector>

template <typename T>
std::ostream & operator<<(std::ostream & stream, const std::vector<T> & what)
{
    stream << "vector(size = " << what.size() << ", capacity = " << what.capacity() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <array>

template <typename T, size_t N>
std::ostream & operator<<(std::ostream & stream, const std::array<T, N> & what)
{
    stream << "array<" << what.size() << ">";
    dumpContainer(stream, what);
    return stream;
}


#include <map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::map<K, V> & what)
{
    stream << "map(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <unordered_map>

template <typename K, typename V>
std::ostream & operator<<(std::ostream & stream, const std::unordered_map<K, V> & what)
{
    stream << "unordered_map(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <set>

template <typename K>
std::ostream & operator<<(std::ostream & stream, const std::set<K> & what)
{
    stream << "set(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <unordered_set>

template <typename K>
std::ostream & operator<<(std::ostream & stream, const std::unordered_set<K> & what)
{
    stream << "unordered_set(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <list>

template <typename K>
std::ostream & operator<<(std::ostream & stream, const std::list<K> & what)
{
    stream << "list(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}


#include <ratio>

template <std::intmax_t Num, std::intmax_t Denom>
std::ostream & operator<<(std::ostream & stream, const std::ratio<Num, Denom> & what)
{
    stream << "ratio<Num=" << Num << ", Denom=" << Denom << ">";
    return stream;
}

#include <chrono>
template <class clock, class duration>
std::ostream & operator<<(std::ostream & stream, const std::chrono::duration<clock, duration> & what)
{
    stream << "chrono::duration<clock=" << clock() << ", duration=" << duration() << ">{" << what.count() << "}";
    return stream;
}

template <class clock, class duration>
std::ostream & operator<<(std::ostream & stream, const std::chrono::time_point<clock, duration> & what)
{
    stream << "chrono::time_point{" << what.time_since_epoch() << "}";
    return stream;
}


#include <memory>

template <class T>
std::ostream & operator<<(std::ostream & stream, const std::shared_ptr<T> & what)
{
    stream << "shared_ptr(use_count = " << what.use_count() << ") {";
    if (what)
        stream << *what;
    else
        stream << "nullptr";
    stream << "}";
    return stream;
}


#include <experimental/optional>

template <class T>
std::ostream & operator<<(std::ostream & stream, const std::experimental::optional<T> & what)
{
    stream << "optional{";
    if (what)
        stream << *what;
    else
        stream << "empty";
    stream << "}";
    return stream;
}


// TODO: add more types
