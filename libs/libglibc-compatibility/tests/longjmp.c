#include <setjmp.h>

int main()
{
    jmp_buf env;
    int val;
    volatile int count = 0;
    val = setjmp(env);
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
        longjmp(env, 42);
    }
    return 1;
}
