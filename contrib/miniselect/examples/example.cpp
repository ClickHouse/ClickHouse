#include <iostream>
#include <vector>

#include "miniselect/median_of_ninthers.h"

int main() {
  std::vector<int> v = {1, 8, 4, 3, 2, 9, 0, 7, 6, 5};
  miniselect::median_of_ninthers_select(v.begin(), v.begin() + 5, v.end());
  for (const int i : v) {
    std::cout << i << ' ';
  }
  return 0;
}

// Compile it `clang++/g++ -I$DIRECTORY/miniselect/include/ example.cpp -std=c++11 -O3 -o example

// Possible output: 0 1 4 3 2 5 8 7 6 9
//                            ^ on the right place
