#include <iostream>
#include <iomanip>
#include <map>

#include <pcg_random.hpp>
#include <Core/Field.h>
#include <Common/HashTable/HashMap.h>
#include <Common/AutoArray.h>
#include <IO/WriteHelpers.h>

#include <Common/Stopwatch.h>


int main(int argc, char ** argv)
{
    pcg64 rng;

    {
        size_t n = 10;
        using T = std::string;
        DB::AutoArray<T> arr(n);

        for (size_t i = 0; i < arr.size(); ++i)
            arr[i] = "Hello, world! " + DB::toString(i);

        for (auto & elem : arr)
            std::cerr << elem << std::endl;
    }

    std::cerr << std::endl;

    {
        size_t n = 10;
        using T = std::string;
        using Arr = DB::AutoArray<T>;
        Arr arr;

        arr.resize(n);
        for (size_t i = 0; i < arr.size(); ++i)
            arr[i] = "Hello, world! " + DB::toString(i);

        for (auto & elem : arr)
            std::cerr << elem << std::endl;

        std::cerr << std::endl;

        Arr arr2 = std::move(arr);

        std::cerr << arr.size() << ", " << arr2.size() << std::endl;  // NOLINT

        for (auto & elem : arr2)
            std::cerr << elem << std::endl;
    }

    std::cerr << std::endl;

    {
        size_t n = 10;
        size_t keys = 10;
        using T = std::string;
        using Arr = DB::AutoArray<T>;
        using Map = std::map<Arr, T>;
        Map map;

        for (size_t i = 0; i < keys; ++i)
        {
            Arr key(n);
            for (size_t j = 0; j < n; ++j)
                key[j] = DB::toString(rng());

            map[std::move(key)] = "Hello, world! " + DB::toString(i);
        }

        for (const auto & kv : map)
        {
            std::cerr << "[";
            for (size_t j = 0; j < n; ++j)
                std::cerr << (j == 0 ? "" : ", ") << kv.first[j];
            std::cerr << "]";

            std::cerr << ":\t" << kv.second << std::endl;
        }

        std::cerr << std::endl;

        Map map2 = std::move(map);

        for (const auto & kv : map2)
        {
            std::cerr << "[";
            for (size_t j = 0; j < n; ++j)
                std::cerr << (j == 0 ? "" : ", ") << kv.first[j];
            std::cerr << "]";

            std::cerr << ":\t" << kv.second << std::endl;
        }
    }

    std::cerr << std::endl;

    {
        size_t n = 10;
        size_t keys = 10;
        using T = std::string;
        using Arr = DB::AutoArray<T>;
        using Vec = std::vector<Arr>;
        Vec vec;

        for (size_t i = 0; i < keys; ++i)
        {
            Arr key(n);
            for (size_t j = 0; j < n; ++j)
                key[j] = DB::toString(rng());

            vec.push_back(std::move(key));
        }

        for (const auto & elem : vec)
        {
            std::cerr << "[";
            for (size_t j = 0; j < n; ++j)
                std::cerr << (j == 0 ? "" : ", ") << elem[j];
            std::cerr << "]" << std::endl;
        }

        std::cerr << std::endl;

        Vec vec2 = std::move(vec);

        for (const auto & elem : vec2)
        {
            std::cerr << "[";
            for (size_t j = 0; j < n; ++j)
                std::cerr << (j == 0 ? "" : ", ") << elem[j];
            std::cerr << "]" << std::endl;
        }
    }

    if (argc == 2 && !strcmp(argv[1], "1"))
    {
        size_t n = 5;
        size_t map_size = 1000000;

        using T = DB::Field;
        T field = std::string("Hello, world");

        using Arr = std::vector<T>;
        using Map = HashMap<UInt64, Arr>;

        Stopwatch watch;

        Map map;
        for (size_t i = 0; i < map_size; ++i)
        {
            Map::LookupResult it;
            bool inserted;

            map.emplace(rng(), it, inserted);
            if (inserted)
            {
                new (&it->getMapped()) Arr(n);

                for (size_t j = 0; j < n; ++j)
                    (it->getMapped())[j] = field;
            }
        }

        std::cerr << std::fixed << std::setprecision(2)
            << "Vector:    Elapsed: " << watch.elapsedSeconds()
            << " (" << map_size / watch.elapsedSeconds() << " rows/sec., "
            << "sizeof(Map::value_type) = " << sizeof(Map::value_type)
            << std::endl;
    }

    {
        size_t n = 10000;
        using Arr = DB::AutoArray<std::string>;
        Arr arr1(n);
        Arr arr2(n);

        for (size_t i = 0; i < n; ++i)
        {
            arr1[i] = "Hello, world! " + DB::toString(i);
            arr2[i] = "Goodbye, world! " + DB::toString(i);
        }

        arr2 = std::move(arr1);
        arr1.resize(n); // NOLINT

        std::cerr
            << "arr1.size(): " << arr1.size() << ", arr2.size(): " << arr2.size() << std::endl
            << "arr1.data(): " << arr1.data() << ", arr2.data(): " << arr2.data() << std::endl
            << "arr1[0]: " << arr1[0] << ", arr2[0]: " << arr2[0] << std::endl;
    }

    return 0;
}
