/// Taken from SMHasher.

#include "AvalancheTest.h"

//-----------------------------------------------------------------------------

double maxBias(std::vector<int> & counts, int reps)
{
    double worst = 0;

    for (int i = 0; i < static_cast<int>(counts.size()); i++)
    {
        double c = static_cast<double>(counts[i]) / static_cast<double>(reps);

        double d = fabs(c * 2 - 1);

        if (d > worst)
        {
            worst = d;
        }
    }

    return worst;
}

//-----------------------------------------------------------------------------
