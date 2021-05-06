// Lizard trivial example : print Library version number
// Copyright : Takayuki Matsuoka & Yann Collet


#include <stdio.h>
#include "lizard_compress.h"

int main(int argc, char** argv)
{
	(void)argc; (void)argv;
    printf("Hello World ! Lizard Library version = %d\n", Lizard_versionNumber());
    return 0;
}
