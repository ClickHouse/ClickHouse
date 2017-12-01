#include <iostream>
#include <fstream>
#include <iomanip>
#include <unordered_map>
#include <sparsehash/dense_hash_map>

#include <Common/Stopwatch.h>

#include <common/StringRef.h>
#include <Common/Arena.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
#include <Common/HashTable/HashMap.h>

int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: program n\n";
        return 1;
    }

    std::cerr << std::fixed << std::setprecision(3);
    std::ofstream devnull("/dev/null");

    DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
    size_t n = atoi(argv[1]);
    size_t elems_show = 1;

    using Vec = std::vector<std::string>;
    using Set = std::unordered_map<std::string, int>;
    using RefsSet = std::unordered_map<StringRef, int, StringRefHash>;
    using DenseSet = google::dense_hash_map<std::string, int>;
    using RefsDenseSet = google::dense_hash_map<StringRef, int, StringRefHash>;
    using RefsHashMap = HashMap<StringRef, int, StringRefHash>;
    Vec vec;

    vec.reserve(n);

    {
        Stopwatch watch;

        std::string s;
        for (size_t i = 0; i < n && !in.eof(); ++i)
        {
            DB::readEscapedString(s, in);
            DB::assertChar('\n', in);
            vec.push_back(s);
        }

        std::cerr << "Read and inserted into vector in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;
    }

    {
        DB::Arena pool;
        Stopwatch watch;
        const char * res = nullptr;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
        {
            const char * tmp = pool.insert(it->data(), it->size());
            if (it == vec.begin())
                res = tmp;
        }

        std::cerr << "Inserted into pool in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        devnull.write(res, 100);
        devnull << std::endl;
    }

    {
        Set set;
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[*it] = 0;

        std::cerr << "Inserted into std::unordered_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (Set::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull << it->first;
            devnull << std::endl;
        }
    }

    {
        RefsSet set;
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[StringRef(*it)] = 0;

        std::cerr << "Inserted refs into std::unordered_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (RefsSet::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }
    }

    {
        DB::Arena pool;
        RefsSet set;
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[StringRef(pool.insert(it->data(), it->size()), it->size())] = 0;

        std::cerr << "Inserted into pool and refs into std::unordered_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (RefsSet::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }
    }

    {
        DenseSet set;
        set.set_empty_key(DenseSet::key_type());
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[*it] = 0;

        std::cerr << "Inserted into google::dense_hash_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (DenseSet::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull << it->first;
            devnull << std::endl;
        }
    }

    {
        RefsDenseSet set;
        set.set_empty_key(RefsDenseSet::key_type());
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[StringRef(it->data(), it->size())] = 0;

        std::cerr << "Inserted refs into google::dense_hash_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (RefsDenseSet::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }
    }

    {
        DB::Arena pool;
        RefsDenseSet set;
        set.set_empty_key(RefsDenseSet::key_type());
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
            set[StringRef(pool.insert(it->data(), it->size()), it->size())] = 0;

        std::cerr << "Inserted into pool and refs into google::dense_hash_map in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (RefsDenseSet::const_iterator it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }
    }

    {
        RefsHashMap set;
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
        {
            RefsHashMap::iterator inserted_it;
            bool inserted;
            set.emplace(StringRef(*it), inserted_it, inserted);
        }

        std::cerr << "Inserted refs into HashMap in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (auto it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }

        //std::cerr << set.size() << ", " << set.getCollisions() << std::endl;
    }

    {
        DB::Arena pool;
        RefsHashMap set;
        Stopwatch watch;

        for (Vec::iterator it = vec.begin(); it != vec.end(); ++it)
        {
            RefsHashMap::iterator inserted_it;
            bool inserted;
            set.emplace(StringRef(pool.insert(it->data(), it->size()), it->size()), inserted_it, inserted);
        }

        std::cerr << "Inserted into pool and refs into HashMap in " << watch.elapsedSeconds() << " sec, "
            << vec.size() / watch.elapsedSeconds() << " rows/sec., "
            << in.count() / watch.elapsedSeconds() / 1000000 << " MB/sec."
            << std::endl;

        size_t i = 0;
        for (auto it = set.begin(); i < elems_show && it != set.end(); ++it, ++i)
        {
            devnull.write(it->first.data, it->first.size);
            devnull << std::endl;
        }
    }

    return 0;
}
