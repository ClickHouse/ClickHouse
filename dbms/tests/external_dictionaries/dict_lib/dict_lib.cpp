#include <iostream>
#include <vector>
#include <cstdint>

struct VectorUint64 {const uint64_t size; const uint64_t * data;};

extern "C" void loadIds(const struct VectorUint64 ids)
{
  std::cerr << "loadIds Runned!!!="<<ids.size<<"\n";
  return;
}

extern "C" void loadAll()
{
  std::cerr << "loadAll Runned!!!"<<"\n";
  return;
}

extern "C" void loadKeys(const struct VectorUint64 requested_rows)
{
  std::cerr << "loadKeys Runned!!!="<<requested_rows.size<<"\n";
  return;
}
