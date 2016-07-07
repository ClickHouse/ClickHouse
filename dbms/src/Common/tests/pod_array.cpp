#include <DB/Common/PODArray.h>
#include <DB/Core/Types.h>
#include <iostream>

#define ASSERT_CHECK(cond, res)						\
do									\
{									\
	if (!(cond))							\
	{								\
		std::cerr << __FILE__ << ":" << __LINE__ << ":"		\
			<< "Assertion " << #cond << " failed.\n";	\
		if ((res)) { (res) = false; }				\
	}								\
} \
while (0)

int main()
{
	using namespace DB;

	static constexpr size_t initial_size = 512;
	static constexpr size_t stack_threshold = 4096;
	using Array = PODArray<UInt8, initial_size, AllocatorWithStackMemory<Allocator<false>, stack_threshold>>;

	Array arr;

	arr.push_back(1);
	arr.push_back(2);
	arr.push_back(3);

	Array arr2;

	arr2 = std::move(arr);

	bool res = true;

	ASSERT_CHECK((arr2.size() == 3), res);
	ASSERT_CHECK((arr2[0] == 1), res);
	ASSERT_CHECK((arr2[1] == 2), res);
	ASSERT_CHECK((arr2[2] == 3), res);

	if (!res)
		std::cerr << "Some errors were found\n";

	return 0;
}
