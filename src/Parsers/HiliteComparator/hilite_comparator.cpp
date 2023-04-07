#include <cassert>

#include <Parsers/HiliteComparator/HiliteComparator.h>


int main(int argc, char* argv[])
{
    assert(argc == 3);
    return !HiliteComparator::are_equal_with_hilites(argv[1], argv[2], true);
}
