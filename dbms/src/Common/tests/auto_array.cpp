#include <iostream>
#include <iomanip>
#include <map>

#include <DB/Core/Field.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/AutoArray.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Common/Stopwatch.h>


int main(int argc, char ** argv)
{
	{
		size_t n = 10;
		typedef std::string T;
		DB::AutoArray<T> arr(n);

		for (size_t i = 0; i < arr.size(); ++i)
			arr[i] = "Hello, world! " + DB::toString(i);

		for (size_t i = 0; i < arr.size(); ++i)
			std::cerr << arr[i] << std::endl;
	}

	std::cerr << std::endl;

	{
		size_t n = 10;
		typedef std::string T;
		DB::AutoArray<T> arr(n, DB::DontInitElemsTag());

		for (size_t i = 0; i < arr.size(); ++i)
			new (arr.place(i)) std::string("Hello, world! " + DB::toString(i));

		for (size_t i = 0; i < arr.size(); ++i)
			std::cerr << arr[i] << std::endl;
	}

	std::cerr << std::endl;
	
	{
		size_t n = 10;
		typedef std::string T;
		typedef DB::AutoArray<T> Arr;
		Arr arr;

		arr.resize(n);
		for (size_t i = 0; i < arr.size(); ++i)
			arr[i] = "Hello, world! " + DB::toString(i);

		for (size_t i = 0; i < arr.size(); ++i)
			std::cerr << arr[i] << std::endl;

		std::cerr << std::endl;

		Arr arr2 = std::move(arr);

		std::cerr << arr.size() << ", " << arr2.size() << std::endl;
		
		for (size_t i = 0; i < arr2.size(); ++i)
			std::cerr << arr2[i] << std::endl;
	}

	std::cerr << std::endl;

	{
		size_t n = 10;
		size_t keys = 10;
		typedef std::string T;
		typedef DB::AutoArray<T> Arr;
		typedef std::map<Arr, T> Map;
		Map map;

		for (size_t i = 0; i < keys; ++i)
		{
			Arr key(n);
			for (size_t j = 0; j < n; ++j)
				key[j] = DB::toString(rand());
			
			map[std::move(key)] = "Hello, world! " + DB::toString(i);
		}

		for (Map::const_iterator it = map.begin(); it != map.end(); ++it)
		{
			std::cerr << "[";
			for (size_t j = 0; j < n; ++j)
				std::cerr << (j == 0 ? "" : ", ") << it->first[j];
			std::cerr << "]";
			
			std::cerr << ":\t" << it->second << std::endl;
		}

		std::cerr << std::endl;

		Map map2 = std::move(map);

		for (Map::const_iterator it = map2.begin(); it != map2.end(); ++it)
		{
			std::cerr << "[";
			for (size_t j = 0; j < n; ++j)
				std::cerr << (j == 0 ? "" : ", ") << it->first[j];
			std::cerr << "]";

			std::cerr << ":\t" << it->second << std::endl;
		}
	}

	std::cerr << std::endl;

	{
		size_t n = 10;
		size_t keys = 10;
		typedef std::string T;
		typedef DB::AutoArray<T> Arr;
		typedef std::vector<Arr> Vec;
		Vec vec;

		for (size_t i = 0; i < keys; ++i)
		{
			Arr key(n);
			for (size_t j = 0; j < n; ++j)
				key[j] = DB::toString(rand());

			vec.push_back(std::move(key));
		}

		for (Vec::const_iterator it = vec.begin(); it != vec.end(); ++it)
		{
			std::cerr << "[";
			for (size_t j = 0; j < n; ++j)
				std::cerr << (j == 0 ? "" : ", ") << (*it)[j];
			std::cerr << "]" << std::endl;
		}

		std::cerr << std::endl;

		Vec vec2 = std::move(vec);

		for (Vec::const_iterator it = vec2.begin(); it != vec2.end(); ++it)
		{
			std::cerr << "[";
			for (size_t j = 0; j < n; ++j)
				std::cerr << (j == 0 ? "" : ", ") << (*it)[j];
			std::cerr << "]" << std::endl;
		}
	}

	size_t n = 5;
	size_t map_size = 1000000;
	typedef DB::Field T;
	T field = std::string("Hello, world");

	if (argc == 2 && !strcmp(argv[1], "1"))
	{
		typedef DB::AutoArray<T> Arr;
		typedef HashMap<UInt64, Arr> Map;

		Stopwatch watch;

		Map map;
		for (size_t i = 0; i < map_size; ++i)
		{
			Map::iterator it;
			bool inserted;

			map.emplace(rand(), it, inserted);
			if (inserted)
			{
				new(&it->second) Arr(n, DB::DontInitElemsTag());

				for (size_t j = 0; j < n; ++j)
					new (it->second.place(j)) T(field);
			}
		}

		std::cerr << std::fixed << std::setprecision(2)
			<< "AutoArray. Elapsed: " << watch.elapsedSeconds()
			<< " (" << map_size / watch.elapsedSeconds() << " rows/sec., "
			<< "sizeof(Map::value_type) = " << sizeof(Map::value_type)
			<< std::endl;
	}

	if (argc == 2 && !strcmp(argv[1], "2"))
	{
		typedef std::vector<T> Arr;
		typedef HashMap<UInt64, Arr> Map;

		Stopwatch watch;

		Map map;
		for (size_t i = 0; i < map_size; ++i)
		{
			Map::iterator it;
			bool inserted;

			map.emplace(rand(), it, inserted);
			if (inserted)
			{
				new(&it->second) Arr(n);

				for (size_t j = 0; j < n; ++j)
					it->second[j] = field;
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
		typedef DB::AutoArray<std::string> Arr;
		Arr arr1(n);
		Arr arr2(n);

		for (size_t i = 0; i < n; ++i)
		{
			arr1[i] = "Hello, world! " + DB::toString(i);
			arr2[i] = "Goodbye, world! " + DB::toString(i);
		}

		arr2 = std::move(arr1);
		arr1.resize(n);

		std::cerr
			<< "arr1.size(): " << arr1.size() << ", arr2.size(): " << arr2.size() << std::endl
			<< "&arr1[0]: " << &arr1[0] << ", &arr2[0]: " << &arr2[0] << std::endl
			<< "arr1[0]: " << arr1[0] << ", arr2[0]: " << arr2[0] << std::endl;
	}

	return 0;
}
