#include <libunwind.h>

unw_word_t
_ReadULEB (unsigned char **dpp)
{
  unsigned shift = 0;
  unw_word_t byte, result = 0;
  unsigned char *bp = *dpp;

  while (1)
    {
      byte = *bp++;
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0)
        break;
      shift += 7;
    }
  *dpp = bp;
  return result;
}
