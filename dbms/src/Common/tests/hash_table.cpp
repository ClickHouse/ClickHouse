#include <iostream>
#include <iomanip>

#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/HashSet.h>


struct SmallGrower : public HashTableGrower
{
	SmallGrower() { size_degree = 1; }
};


int main(int argc, char ** argv)
{
	{
		typedef HashSet<int, DefaultHash<int>, SmallGrower> Cont;
		Cont cont;

		cont.insert(1);
		cont.insert(2);

		Cont::iterator it;
		bool inserted;

		cont.emplace(3, it, inserted);
		std::cerr << inserted << ", " << *it << std::endl;

		cont.emplace(3, it, inserted);
		std::cerr << inserted << ", " << *it << std::endl;

		for (auto x : cont)
			std::cerr << x << std::endl;

		std::string dump;
		{
			DB::WriteBufferFromString wb(dump);
			cont.writeText(wb);
		}

		std::cerr << "dump: " << dump << std::endl;
	}

	{
		typedef HashMap<int, std::string, DefaultHash<int>, SmallGrower> Cont;
		Cont cont;

		cont.insert(std::make_pair(1, "Hello, world!"));

		for (auto x : cont)
			std::cerr << x.first << " -> " << x.second << std::endl;
	}

	return 0;
}
