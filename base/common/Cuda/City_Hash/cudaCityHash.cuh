#pragma once


//little endian is assumed for all the code bellow

namespace CityHash_v1_0_2_cuda
{

//typedef std::pair<uint64_t, uint64_t> uint128;
typedef struct
{
    uint64_t first;
    uint64_t second;
} uint128;


static const uint64_t k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t k1 = 0xb492b66fbe98f273ULL;
static const uint64_t k2 = 0x9ae16a3b2f90404fULL;
static const uint64_t k3 = 0xc949d7c7509e6557ULL;

inline __device__ uint64_t Uint128Low64(const uint128& x) { return x.first; }
inline __device__ uint64_t Uint128High64(const uint128& x) { return x.second; }


inline __device__ uint64_t Fetch64(const char *p) 
{
    //return *(uint64_t *)(p);
    uint64_t result;
    size_t result_size=sizeof(result);
    //memcpy(&result, p, result_size);
    //result = *(uint64_t *)(p);  //this causes CUDA_EXCEPTION_6 Warp Misaligned Address
    //cudaMemcpy(&result, p, result_size, cudaMemcpyDeviceToDevice);
    memcpy(&result, p, result_size); //how bad is it?
    return result;
}

inline __device__ uint32_t Fetch32(const char *p) 
{
    //return *(uint32_t *)(p);
    uint32_t result;
    //size_t result_size=sizeof(result);
    //memcpy(&result, p, result_size);
    //result = *(uint32_t *)(p);  //this causes CUDA_EXCEPTION_6 Warp Misaligned Address
    size_t result_size=sizeof(result);
    //cudaMemcpy(&result, p, result_size, cudaMemcpyDeviceToDevice);
    memcpy(&result, p, result_size);  //how bad is it?
    return result;    
}



inline __device__ uint64_t Rotate(uint64_t val, int shift) 
{
    // Avoid shifting by 64: doing so yields an undefined result.
    return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}


inline __device__ uint64_t RotateByAtLeast1(uint64_t val, int shift) 
{
    return (val >> shift) | (val << (64 - shift));
}


inline __device__  uint64_t ShiftMix(uint64_t val) 
{
    return val ^ (val >> 47);
}

inline __device__ uint64_t Hash128to64(const uint128& x) {
    // Murmur-inspired hashing.
    const uint64_t kMul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (Uint128Low64(x) ^ Uint128High64(x)) * kMul;
    a ^= (a >> 47);
    uint64_t b = (Uint128High64(x) ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

inline __device__  uint64_t HashLen16(uint64_t u, uint64_t v) 
{
    uint128 hhh;
    hhh.first=u;
    hhh.second=v;
    return Hash128to64(hhh);
}

inline __device__  uint64_t HashLen0to16(const char *s, size_t len) 
{
    if (len > 8){
        uint64_t a = Fetch64(s);
        const char *s_shifted = s + len - 8;
        uint64_t b = Fetch64(s_shifted);

        return HashLen16(a, RotateByAtLeast1(b + len, len)) ^ b;
    }
    if (len >= 4){
        uint64_t a = Fetch32(s);
        const char *p_updated=s + len - 4;
        uint64_t b = Fetch32(p_updated);
        uint64_t c = len + (a << 3);
        return HashLen16(c, b);
    }
    if (len > 0){
        uint8_t a = s[0];
        uint8_t b = s[len >> 1];
        uint8_t c = s[len - 1];
        uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
        uint32_t z = len + (static_cast<uint32_t>(c) << 2);
        return ShiftMix(y * k2 ^ z * k3) * k2;
    }
    return k2;
}


// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
inline __device__ uint64_t HashLen17to32(const char *s, size_t len)
{
    uint64_t a = Fetch64(s) * k1;
    uint64_t b = Fetch64(s + 8);
    uint64_t c = Fetch64(s + len - 8) * k2;
    uint64_t d = Fetch64(s + len - 16) * k0;
    return HashLen16(Rotate(a - b, 43) + Rotate(c, 30) + d, a + Rotate(b ^ k3, 20) - c + len);
}


// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
inline __device__  uint128 WeakHashLen32WithSeeds(uint64_t w, uint64_t x, uint64_t y, uint64_t z, uint64_t a, uint64_t b)//pair<uint64, uint64>
{
    a += w;
    b = Rotate(b + a + z, 21);
    uint64_t c = a;
    a += x;
    a += y;
    b += Rotate(a, 44);
    uint128 ret;
    ret.first=a + z;
    ret.second=b + c;
    //return make_pair(a + z, b + c);
    return ret;
}


// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
inline __device__  uint128 WeakHashLen32WithSeeds(const char* s, uint64_t a, uint64_t b) //pair<uint64, uint64>
{
    return WeakHashLen32WithSeeds(Fetch64(s),
                                  Fetch64(s + 8),
                                  Fetch64(s + 16),
                                  Fetch64(s + 24),
                                  a,
                                  b);
}



// Return an 8-byte hash for 33 to 64 bytes.
inline __device__ uint64_t HashLen33to64(const char *s, size_t len)
{
    uint64_t z = Fetch64(s + 24);

    //uint64_t a = Fetch64(s) + (len + Fetch64(s + len - 16)) * k0;
    
    uint64_t a=Fetch64(s);
    uint64_t a_t1=Fetch64(s + len - 16);
    a=a+(len+a_t1)*k0;
    uint64_t b = Rotate(a + z, 52);
    uint64_t c = Rotate(a, 37);
    a += Fetch64(s + 8);
    c += Rotate(a, 7);
    a += Fetch64(s + 16);
    uint64_t vf = a + z;
    uint64_t vs = b + Rotate(a, 31) + c;
    a = Fetch64(s + 16) + Fetch64(s + len - 32);
    z = Fetch64(s + len - 8);
    b = Rotate(a + z, 52);
    c = Rotate(a, 37);
    a += Fetch64(s + len - 24);
    c += Rotate(a, 7);
    a += Fetch64(s + len - 16);
    uint64_t wf = a + z;
    uint64_t ws = b + Rotate(a, 31) + c;
    uint64_t r = ShiftMix((vf + ws) * k2 + (wf + vs) * k0);
    return ShiftMix(r * k0 + vs) * k2;
}



__device__ uint64_t cudaCityHash64(const char *s, size_t len)
{
    if (len <= 32) {
        if (len <= 16) {
            return HashLen0to16(s, len);
        } 
        else{
            return HashLen17to32(s, len);
        }
    }else if(len <= 64){
        return HashLen33to64(s, len);
    }

// For strings over 64 bytes we hash the end first, and then as we
// loop we keep 56 bytes of state: v, w, x, y, and z.
    uint64_t x = Fetch64(s);
    uint64_t y = Fetch64(s + len - 16) ^ k1;
    uint64_t z = Fetch64(s + len - 56) ^ k0;
    uint128 v = WeakHashLen32WithSeeds(s + len - 64, len, y);
    uint128 w = WeakHashLen32WithSeeds(s + len - 32, len * k1, k0);
    z += ShiftMix(v.second) * k1;
    x = Rotate(z + x, 39) * k1;
    y = Rotate(y, 33) * k1;

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
    len = (len - 1) & ~static_cast<size_t>(63);
    do {
        x = Rotate(x + y + v.first + Fetch64(s + 16), 37) * k1;
        y = Rotate(y + v.second + Fetch64(s + 48), 42) * k1;
        x ^= w.second;
        y ^= v.first;
        z = Rotate(z ^ w.first, 33);
        v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
        w = WeakHashLen32WithSeeds(s + 32, z + w.second, y);
        //std::swap(z, x);
        uint64_t temp=x;
        x=z;
        z=temp;
        s += 64;
        len -= 64;
    }while (len != 0);

    return HashLen16(HashLen16(v.first, w.first) + ShiftMix(y) * k1 + z, HashLen16(v.second, w.second) + x);

}



}