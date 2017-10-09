#include <setjmp.h>

int main()
{
    sigjmp_buf env;
    int val;
    volatile int count = 0;
    val = sigsetjmp(env, 0);
    ++count;
    if (count == 1 && val != 0)
    {
        return 1;
    }
    if (count == 2 && val == 42)
    {
        return 0;
    }
    if (count == 1)
    {
        siglongjmp(env, 42);
    }
    return 1;
}
