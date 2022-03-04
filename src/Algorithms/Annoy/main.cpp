#include "annoy.h"

int main()
{
  std::vector<std::vector<double>> points = {{0, 0},{0, 1}, {1, 0}, {1, 1}};
  Annoy annoy(points);
  return 0;
}