#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <google/dense_hash_map>
#include <google/sparse_hash_map>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Types.h>
#include <DB/Interpreters/HashMap.h>


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);
	size_t m = atoi(argv[2]);
	std::vector<DB::UInt64> data(n);

	{
		Stopwatch watch;
		for (size_t i = 0; i < n; ++i)
			data[i] = rand() % m;

		for (size_t i = 0; i < n; ++i)
			if (data[i] == 0)
				data[i] = 1;

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		DB::HashMap<DB::UInt64, DB::UInt64> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		std::tr1::unordered_map<DB::UInt64, DB::UInt64> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		google::dense_hash_map<DB::UInt64, DB::UInt64> map;
		map.set_empty_key(0);
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	/*{
		Stopwatch watch;

		google::sparse_hash_map<DB::UInt64, DB::UInt64> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/
	
	return 0;
}
