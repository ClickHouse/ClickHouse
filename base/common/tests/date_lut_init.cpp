#include <common/DateLUT.h>

/// Позволяет проверить время инициализации DateLUT.
int main(int, char **)
{
    DateLUT::instance();
    return 0;
}
