/// Taken from SMHasher.

#pragma once

#include <cstdint>

//-----------------------------------------------------------------------------
// Xorshift RNG based on code by George Marsaglia
// http://en.wikipedia.org/wiki/Xorshift

struct Rand
{
  uint32_t x;
  uint32_t y;
  uint32_t z;
  uint32_t w;

  Rand()
  {
    reseed(static_cast<uint32_t>(0));
  }

  explicit Rand( uint32_t seed )
  {
    reseed(seed);
  }

  void reseed ( uint32_t seed )
  {
    x = 0x498b3bc5 ^ seed;
    y = 0;
    z = 0;
    w = 0;

    for(int i = 0; i < 10; i++) mix();
  }

  void reseed ( uint64_t seed )
  {
    x = 0x498b3bc5 ^ static_cast<uint32_t>(seed >>  0);
    y = 0x5a05089a ^ static_cast<uint32_t>(seed >> 32);
    z = 0;
    w = 0;

    for(int i = 0; i < 10; i++) mix();
  }

  //-----------------------------------------------------------------------------

  void mix ( void )
  {
    uint32_t t = x ^ (x << 11);
    x = y; y = z; z = w;
    w = w ^ (w >> 19) ^ t ^ (t >> 8);
  }

  uint32_t rand_u32 ( void )
  {
    mix();

    return x;
  }

  uint64_t rand_u64 ( void )
  {
    mix();

    uint64_t a = x;
    uint64_t b = y;

    return (a << 32) | b;
  }

  void rand_p ( void * blob, int bytes )
  {
    uint32_t * blocks = reinterpret_cast<uint32_t*>(blob);

    while(bytes >= 4)
    {
      blocks[0] = rand_u32();
      blocks++;
      bytes -= 4;
    }

    uint8_t * tail = reinterpret_cast<uint8_t*>(blocks);

    for(int i = 0; i < bytes; i++)
    {
      tail[i] = static_cast<uint8_t>(rand_u32());
    }
  }
};

//-----------------------------------------------------------------------------

extern Rand g_rand1;

inline uint32_t rand_u32 ( void ) { return g_rand1.rand_u32(); }
inline uint64_t rand_u64 ( void ) { return g_rand1.rand_u64(); }

inline void rand_p ( void * blob, int bytes )
{
  uint32_t * blocks = static_cast<uint32_t*>(blob);

  while(bytes >= 4)
  {
    *blocks++ = rand_u32();
    bytes -= 4;
  }

  uint8_t * tail = reinterpret_cast<uint8_t*>(blocks);

  for(int i = 0; i < bytes; i++)
  {
    tail[i] = static_cast<uint8_t>(rand_u32());
  }
}

//-----------------------------------------------------------------------------
