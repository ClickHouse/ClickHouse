#include <openssl/opensslv.h>
#include <stdio.h>

int main()
{
  printf("%s", LIBRESSL_VERSION_TEXT);
}
