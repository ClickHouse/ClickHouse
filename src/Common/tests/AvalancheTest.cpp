/// Taken from SMHasher.

#include "AvalancheTest.h"

//-----------------------------------------------------------------------------

double maxBias(std::vector<int> & counts, int reps)
{
    double worst = 0;

    for (auto count : counts)
    {
        double c = static_cast<double>(count) / static_cast<double>(reps);

        double d = fabs(c * 2 - 1);

        if (d > worst)
        {
            worst = d;
        }
    }

    return worst;
}

//-----------------------------------------------------------------------------
