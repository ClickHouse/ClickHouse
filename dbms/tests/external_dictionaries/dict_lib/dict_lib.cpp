#include <iostream>
#include <vector>

typedef unsigned long      UInt64; // TEST ONLY

extern "C" void loadIds(const std::vector<UInt64> & ids)
{
  std::cerr << "loadIds Runned!!!="<<ids.size()<<"\n";
  return;
}
