/// Взято из SMHasher.

#include "AvalancheTest.h"

//-----------------------------------------------------------------------------

void PrintAvalancheDiagram ( int x, int y, int reps, double scale, int * bins )
{
  const char * symbols = ".123456789X";

  for(int i = 0; i < y; i++)
  {
    printf("[");
    for(int j = 0; j < x; j++)
    {
      int k = (y - i) -1;

      int bin = bins[k + (j*y)];

      double b = double(bin) / double(reps);
      b = fabs(b*2 - 1);

      b *= scale;

      int s = (int)floor(b*10);

      if(s > 10) s = 10;
      if(s < 0) s = 0;

      printf("%c",symbols[s]);
    }

    printf("]\n");
  }
}

//----------------------------------------------------------------------------

double maxBias ( std::vector<int> & counts, int reps )
{
  double worst = 0;

  for(int i = 0; i < (int)counts.size(); i++)
  {
    double c = double(counts[i]) / double(reps);

    double d = fabs(c * 2 - 1);
      
    if(d > worst)
    {
      worst = d;
    }
  }

  return worst;
}

//-----------------------------------------------------------------------------
