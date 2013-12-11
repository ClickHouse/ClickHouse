#include <iostream>
#include <DB/Storages/MergeTree/PKCondition.h>


int main(int argc, const char ** argv)
{
	using namespace DB;

	Range range1;
	Range range2;

	range1.left = UInt64(101);
	range1.right = UInt64(101);
	range1.left_bounded = true;
	range1.right_bounded = true;
	range1.left_included = true;
	range1.right_included = true;

	range2.left = Int64(100);
	range2.left_bounded = true;
	range2.left_included = true;

	std::cerr << range1.toString() << (range1.intersectsRange(range2) ? " intersects " : " not intersects ") << range2.toString() << std::endl;

	return 0;
}
