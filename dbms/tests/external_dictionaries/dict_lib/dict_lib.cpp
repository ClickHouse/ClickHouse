#include <iostream>
#include <vector>
#include <cstdint>

typedef uint64_t UInt64;

extern "C" void loadIds(const std::vector<UInt64> & ids)
{
  std::cerr << "loadIds Runned!!!="<<ids.size()<<"\n";
  return;
}

extern "C" void loadAll()
{
  std::cerr << "loadAll Runned!!!"<<"\n";
  return;
}

extern "C" void loadKeys(const std::vector<std::size_t> & requested_rows)
{
  std::cerr << "loadKeys Runned!!!="<<requested_rows.size()<<"\n";
  return;
}
