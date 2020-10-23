#include <common/DateLUT.h>

/// Позволяет проверить время инициализации DateLUT.
int main(int argc, char ** argv)
{
    DateLUT::instance();
    return 0;
}
