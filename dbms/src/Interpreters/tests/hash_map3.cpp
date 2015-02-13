#include <iostream>

#define DBMS_HASH_MAP_DEBUG_RESIZES
#define DBMS_HASH_MAP_COUNT_COLLISIONS


#include <string.h>

#include <malloc.h>

#include <utility>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>

#include <DB/Core/StringRef.h>

#include <DB/Common/HashTable/HashMap.h>


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<>,
	typename Allocator = HashTableAllocator
>
class HashMapWithDump : public HashMap<Key, Mapped, Hash, Grower, Allocator>
{
public:
	void dump() const
	{
		for (size_t i = 0; i < this->grower.bufSize(); ++i)
		{
			if (this->buf[i].isZero(*this))
				std::cerr << "[    ]";
			else
				std::cerr << '[' << this->buf[i].getValue().first.data << ", " << this->buf[i].getValue().second << ']';
		}
		std::cerr << std::endl;
	}
};


struct SimpleHash
{
	size_t operator() (UInt64 x) const { return x; }
	size_t operator() (StringRef x) const { return DB::parse<UInt64>(x.data); }
};

struct Grower : public HashTableGrower<2>
{
	void increaseSize()
	{
		++size_degree;
	}
};

int main(int argc, char ** argv)
{
	typedef HashMapWithDump<
		StringRef,
		UInt64,
		SimpleHash,
		Grower,
		HashTableAllocatorWithStackMemory<4 * 24> > Map;

	Map map;

	map.dump();
	std::cerr << "size: " << map.size() << std::endl;
	map[StringRef("1", 1)] = 1;
	map.dump();
	std::cerr << "size: " << map.size() << std::endl;
	map[StringRef("9", 1)] = 1;
	map.dump();
	std::cerr << "size: " << map.size() << std::endl;
	std::cerr << "Collisions: " << map.getCollisions() << std::endl;
	map[StringRef("3", 1)] = 2;
	map.dump();
	std::cerr << "size: " << map.size() << std::endl;
	std::cerr << "Collisions: " << map.getCollisions() << std::endl;

	for (auto x : map)
		std::cerr << x.first.toString() << " -> " << x.second << std::endl;

	return 0;
}
