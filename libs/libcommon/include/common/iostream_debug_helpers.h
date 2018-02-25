#pragma once
#include <iostream>

#include <array>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ratio>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// TODO: https://stackoverflow.com/questions/16464032/how-to-enhance-this-variable-dumping-debug-macro-to-be-variadic
#define DUMPS(VAR) #VAR " = " << (VAR)
#define DUMPHEAD std::cerr << __FILE__ << ":" << __LINE__ << " "
#define DUMP(V1) DUMPHEAD << DUMPS(V1) << "\n";
#define DUMP2(V1, V2) DUMPHEAD << DUMPS(V1) << ", " << DUMPS(V2) << "\n";
#define DUMP3(V1, V2, V3) DUMPHEAD << DUMPS(V1) << ", " << DUMPS(V2) << ", " << DUMPS(V3) << "\n";
#define DUMP4(V1, V2, V3, V4) DUMPHEAD << DUMPS(V1) << ", " << DUMPS(V2) << ", " << DUMPS(V3)<< ", " << DUMPS(V4) << "\n";
#define DUMP5(V1, V2, V3, V4, V5) DUMPHEAD << DUMPS(V1) << ", " << DUMPS(V2) << ", " << DUMPS(V3)<< ", " << DUMPS(V4) << ", " << DUMPS(V5) << "\n";
#define DUMP6(V1, V2, V3, V4, V5, V6) DUMPHEAD << DUMPS(V1) << ", " << DUMPS(V2) << ", " << DUMPS(V3)<< ", " << DUMPS(V4) << ", " << DUMPS(V5) << ", " << DUMPS(V6) << "\n";


namespace std
{

template <typename K, typename V>
ostream & operator<<(ostream & stream, const pair<K, V> & what)
{
    stream << "pair{" << what.first << ", " << what.second << "}";
    return stream;
}

template <typename T>
void dumpContainer(ostream & stream, const T & container)
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

template <typename T>
ostream & operator<<(ostream & stream, const vector<T> & what)
{
    stream << "vector(size = " << what.size() << ", capacity = " << what.capacity() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename T, size_t N>
ostream & operator<<(ostream & stream, const array<T, N> & what)
{
    stream << "array<" << what.size() << ">";
    dumpContainer(stream, what);
    return stream;
}

template <typename K, typename V>
ostream & operator<<(ostream & stream, const map<K, V> & what)
{
    stream << "map(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K, typename V>
ostream & operator<<(ostream & stream, const multimap<K, V> & what)
{
    stream << "multimap(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K, typename V>
ostream & operator<<(ostream & stream, const unordered_map<K, V> & what)
{
    stream << "unordered_map(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K, typename V>
ostream & operator<<(ostream & stream, const unordered_multimap<K, V> & what)
{
    stream << "unordered_multimap(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K>
ostream & operator<<(ostream & stream, const set<K> & what)
{
    stream << "set(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K>
ostream & operator<<(ostream & stream, const multiset<K> & what)
{
    stream << "multiset(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K>
ostream & operator<<(ostream & stream, const unordered_set<K> & what)
{
    stream << "unordered_set(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K>
ostream & operator<<(ostream & stream, const unordered_multiset<K> & what)
{
    stream << "unordered_multiset(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <typename K>
ostream & operator<<(ostream & stream, const list<K> & what)
{
    stream << "list(size = " << what.size() << ")";
    dumpContainer(stream, what);
    return stream;
}

template <intmax_t Num, intmax_t Denom>
ostream & operator<<(ostream & stream, [[maybe_unused]] const ratio<Num, Denom> & what)
{
    stream << "ratio<Num=" << Num << ", Denom=" << Denom << ">";
    return stream;
}

template <typename clock, typename duration>
ostream & operator<<(ostream & stream, const chrono::duration<clock, duration> & what)
{
    stream << "chrono::duration<clock=" << clock() << ", duration=" << duration() << ">{" << what.count() << "}";
    return stream;
}

template <typename clock, typename duration>
ostream & operator<<(ostream & stream, const chrono::time_point<clock, duration> & what)
{
    stream << "chrono::time_point{" << what.time_since_epoch() << "}";
    return stream;
}

template <typename T>
ostream & operator<<(ostream & stream, const shared_ptr<T> & what)
{
    stream << "shared_ptr(" << what.get() << ", use_count = " << what.use_count() << ") {";
    if (what)
        stream << *what;
    else
        stream << "nullptr";
    stream << "}";
    return stream;
}

template <typename T>
ostream & operator<<(ostream & stream, const unique_ptr<T> & what)
{
    stream << "unique_ptr(" << what.get() << ") {";
    if (what)
        stream << *what;
    else
        stream << "nullptr";
    stream << "}";
    return stream;
}

template <typename T>
ostream & operator<<(ostream & stream, const optional<T> & what)
{
    stream << "optional{";
    if (what)
        stream << *what;
    else
        stream << "empty";
    stream << "}";
    return stream;
}

class exception;
ostream & operator<<(ostream & stream, const exception & what);

// TODO: add more types

}
