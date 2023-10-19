#include <string.h>

int main()
{
    // We can't test "char *p = strerror_r()" because that only causes a
    // compiler warning when strerror_r returns an integer.
    char *buf = 0;
    int i = strerror_r(0, buf, 100);
    return i;
}
