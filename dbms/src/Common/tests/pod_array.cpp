#include <iostream>
#include <iomanip>
#include <vector>
#include <cassert>

#include <Poco/NumberFormatter.h>

#include <DB/Core/Types.h>
#include <DB/Common/PODArray.h>

#include <statdaemons/Stopwatch.h>


int main(int argc, char ** argv)
{
	try
	{
		typedef UInt16 Value;
		typedef DB::PODArray<Value> Arr;

		std::cerr << std::fixed << std::setprecision(3) << std::endl;

		{
			Arr arr;
			assert(arr.size() == 0);

			arr.push_back(1);
			assert(arr.size() == 1);

			arr.clear();
			assert(arr.size() == 0);

			arr.reserve(1000000);
			assert(arr.capacity() >= 1000000);

			arr.resize(10);
			assert(arr.size() == 10);
			assert(arr.capacity() >= 1000000);

			arr.front() = 12;
			arr.back() = 34;
			arr[5] = 56;

			Arr arr2(10);
			arr2.front() = 12;
			arr2.back() = 34;
			arr2[5] = 56;

			arr.insert(arr2.begin(), arr2.end());

			assert(arr.size() == 20);
			assert(arr.capacity() >= 1000000);
			assert(arr[0] == 12);
			assert(arr[9] == 34);
			assert(arr[5] == 56);
			assert(arr[10] == 12);
			assert(arr[19] == 34);
			assert(arr[15] == 56);

			Arr().swap(arr);
			assert(arr.size() == 0);
			assert(arr.capacity() < 1000000);

			arr.resize(4090);
			arr.insert(arr2.begin(), arr2.end());
			assert(arr.size() == 4100);
			assert(arr.capacity() == 8192);
			assert(arr[4090] == 12);
			assert(arr[4099] == 34);
			assert(arr[4095] == 56);
		}

		{
			Arr arr(4096);
			arr.push_back(123);
			std::cerr << "size: " << arr.size() << ", capacity: " << arr.capacity() << std::endl;

			for (Arr::const_iterator it = arr.begin(); it != arr.end(); ++it)
				std::cerr << (it != arr.begin() ? ", " : "") << *it;
			std::cerr << std::endl;
		}

		{
			Arr arr;

			for (size_t i = 0; i < 10000; ++i)
				arr.push_back(i);
			std::cerr << "size: " << arr.size() << ", capacity: " << arr.capacity() << std::endl;

			for (Arr::const_iterator it = arr.begin(); it != arr.end(); ++it)
				std::cerr << (it != arr.begin() ? ", " : "") << *it;
			std::cerr << std::endl;
		}

		{
			Arr arr;
			arr.resize(10000);

			for (size_t i = 0; i < 10000; ++i)
				arr[i] = i;
			std::cerr << "size: " << arr.size() << ", capacity: " << arr.capacity() << std::endl;

			for (Arr::const_iterator it = arr.begin(); it != arr.end(); ++it)
				std::cerr << (it != arr.begin() ? ", " : "") << *it;
			std::cerr << std::endl;
		}

		{
			Arr arr;
			size_t n = 100000000;

			Stopwatch watch;

			for (size_t i = 0; i < n; ++i)
				arr.push_back(i);

			watch.stop();

			std::cerr << "size: " << arr.size() << ", capacity: " << arr.capacity() << std::endl;

			std::cerr << "PODArray: " << watch.elapsedSeconds() << " sec., "
				<< n / watch.elapsedSeconds() << " elems/sec., "
				<< n * sizeof(Value) / watch.elapsedSeconds() / 1000000 << " MB/sec."
				<< std::endl;
		}

		{
			std::vector<Value> arr;
			size_t n = 100000000;

			Stopwatch watch;

			for (size_t i = 0; i < n; ++i)
				arr.push_back(i);

			watch.stop();

			std::cerr << "size: " << arr.size() << ", capacity: " << arr.capacity() << std::endl;

			std::cerr << "std::vector: " << watch.elapsedSeconds() << " sec., "
				<< n / watch.elapsedSeconds() << " elems/sec., "
				<< n * sizeof(Value) / watch.elapsedSeconds() / 1000000 << " MB/sec."
				<< std::endl;
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
