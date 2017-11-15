[![Build Status](https://travis-ci.org/Tessil/hopscotch-map.svg?branch=master)](https://travis-ci.org/Tessil/hopscotch-map) [![Build status](https://ci.appveyor.com/api/projects/status/e97rjkcn3qwrhpvf/branch/master?svg=true)](https://ci.appveyor.com/project/Tessil/hopscotch-map/branch/master)

## A C++ implementation of a fast hash map using hopscotch hashing

The hopscotch-map library is a C++ implementation of a fast hash map and hash set using open-addressing and hopscotch hashing to resolve collisions. It is a cache-friendly data structure offering better performances than `std::unordered_map` in most cases and is closely similar to `google::dense_hash_map` while using less memory and providing more functionalities.

The library provides four classes: `tsl::hopscotch_map`, `tsl::hopscotch_set`, `tsl::hopscotch_sc_map` and `tsl::hopscotch_sc_set`. The `tsl::hopscotch_sc_map` and `tsl::hopscotch_sc_set` classes have an additional requirement for the key, must be `LessThanComparable`, but provide a better upper bound, see [details](https://github.com/Tessil/hopscotch-map#deny-of-service-dos-attack) in example. Nonetheless, `tsl::hopscotch_map` and `tsl::hopscotch_set` should be sufficient in most cases and should be your default pick as they perform better in general.

An overview of hopscotch hashing and some implementation details may be found [here](https://tessil.github.io/2016/08/29/hopscotch-hashing.html).

A **benchmark** of `tsl::hopscotch_map` against other hash maps may be found [there](https://tessil.github.io/2016/08/29/benchmark-hopscotch-map.html).

**Note**: By default the library uses a power of two for the size of its buckets array to take advantage of the [fast modulo](https://en.wikipedia.org/wiki/Modulo_operation#Performance_issues). For good performance, it requires the hash table to have a well-distributed hash function. If you encounter performance issues check the [GrowthPolicy](https://github.com/Tessil/hopscotch-map#growth-policy) section to change the default behaviour or change your hash function.

### Key features
- Header-only library, just include [src/](src/) to your include path and you are ready to go.
- Fast hash table, see [benchmark](https://tessil.github.io/2016/08/29/benchmark-hopscotch-map.html) for some numbers.
- Support for move-only and non-default constructible key/value.
- Support for heterogeneous lookups (e.g. if you have a map that uses `std::unique_ptr<int>` as key, you could use an `int*` or a `std::uintptr_t` as key parameter to `find`, see [example](https://github.com/Tessil/hopscotch-map#heterogeneous-lookups)).
- No need to reserve any sentinel value from the keys.
- Possibility to store the hash value on insert for faster rehash and lookup if the hash or the key equal functions are expensive to compute (see the [StoreHash](https://tessil.github.io/hopscotch-map/doc/html/classtsl_1_1hopscotch__map.html#details) template parameter).
- If the hash is known before a lookup, it is possible to pass it as parameter to speed-up the lookup.
- The `tsl::hopscotch_sc_map` and `tsl::hopscotch_sc_set` provide a worst-case of O(log n) on lookup and delete making these classes resistant to hash table Deny of Service (DoS) attacks (see [details](https://github.com/Tessil/hopscotch-map#deny-of-service-dos-attack) in example).
- API closely similar to `std::unordered_map` and `std::unordered_set`.

### Differences compare to `std::unordered_map`
`tsl::hopscotch_map` tries to have an interface similar to `std::unordered_map`, but some differences exist.
- Iterator invalidation on insert doesn't behave in the same way (see [API](https://tessil.github.io/hopscotch-map/doc/html/classtsl_1_1hopscotch__map.html#details) for details).
- References and pointers to keys or values in the map are invalidated in the same way as iterators to these keys-values on insert.
- The size of the bucket array in the map grows by a factor of two, the size will always be a power of two, which may be a too steep growth rate for some purposes. The growth policy is modifiable (see the [`GrowthPolicy`](https://github.com/Tessil/hopscotch-map#growth-policy) template parameter) but it may reduce the speed of the hash map.
- For iterators, `operator*()` and `operator->()` return a reference and a pointer to `const std::pair<Key, T>` instead of `std::pair<const Key, T>` making the value `T` not modifiable. To modify the value you have to call the `value()` method of the iterator to get a mutable reference. Example:
```c++
tsl::hopscotch_map<int, int> map = {{1, 1}, {2, 1}, {3, 1}};
for(auto it = map.begin(); it != map.end(); ++it) {
    //it->second = 2; // Illegal
    it.value() = 2; // Ok
}
```
- Move-only types must have a nothrow move constructor (with open addressing, it is not possible to keep the strong exception guarantee on rehash if the move constructor may throw).
- No support for some buckets related methods (like bucket_size, bucket, ...).

These differences also apply between `std::unordered_set` and `tsl::hopscotch_set`.

Thread-safety and exceptions guarantees are the same as `std::unordered_map/set`.

### Differences compare to `google::dense_hash_map`
`tsl::hopscotch_map` has comparable performances to `google::dense_hash_map` (see [benchmark](https://tessil.github.io/2016/08/29/benchmark-hopscotch-map.html)), but come with some advantages.
- There is no need to reserve sentinel values for the key as it is required by `google::dense_hash_map` where you need to have a sentinel for empty and deleted keys.
- The type of the value in the map doesn't need a default constructor.
- The key and the value of the map don't need a copy constructor/operator, move-only types are supported.
- It uses less memory for its speed as it can sustain a load factor of 0.95 (which is the default value in the library compare to the 0.5 of `google::dense_hash_map`) while keeping good performances.

### Growth policy

By default `tsl::hopscotch_map/set` uses `tsl::power_of_two_growth_policy` as `GrowthPolicy`. This policy keeps the size of the map to a power of two by doubling the size of the map when a rehash is required. It allows the map to avoid the usage of the slow modulo operation, instead of <code>hash % 2<sup>n</sup></code>, it uses <code>hash & (2<sup>n</sup> - 1)</code>.

This may cause a lot of collisions with a poor hash function as the modulo just masks the most significant bits.

If you encounter poor performances, check `overflow_size()`. If it is not zero, you may have a lot of collisions due to a common pattern in the least significant bits. Either change the hash function for something more uniform or use `tsl::prime_growth_policy` which keeps the size of the map to a prime size.

You can also use `tsl::mod_growth_policy` if you want a more configurable growth rate or you could even define your own policy (see [API](https://tessil.github.io/hopscotch-map/doc/html/classtsl_1_1hopscotch__map.html#details)).

A bad distribution may lead to a runtime complexity of O(n) for lookups. Unfortunately it is sometimes difficult to guard yourself against it (e.g. DoS attack on the hash map). If needed, check `tsl::hopscotch_sc_map/set` which offer a worst-case scenario of O(log n) on lookups, see [details](https://github.com/Tessil/hopscotch-map#deny-of-service-dos-attack) in example.

### Installation
To use hopscotch-map, just add the [src/](src/) directory to your include path. It is a **header-only** library.

The code should work with any C++11 standard-compliant compiler and has been tested with GCC 4.8.4, Clang 3.5.0 and Visual Studio 2015.

To run the tests you will need the Boost Test library and CMake. 

```bash
git clone https://github.com/Tessil/hopscotch-map.git
cd hopscotch-map
mkdir build
cd build
cmake ..
make
./test_hopscotch_map 
```


### Usage
The API can be found [here](https://tessil.github.io/hopscotch-map/doc/html/). 

All methods are not documented yet, but they replicate the behaviour of the ones in `std::unordered_map` and `std::unordered_set`, except if specified otherwise.

### Example
```c++
#include <cstdint>
#include <iostream>
#include <string>
#include "hopscotch_map.h"
#include "hopscotch_set.h"

int main() {
    tsl::hopscotch_map<std::string, int> map = {{"a", 1}, {"b", 2}};
    map["c"] = 3;
    map["d"] = 4;
    
    map.insert({"e", 5});
    map.erase("b");
    
    for(auto it = map.begin(); it != map.end(); ++it) {
        //it->second += 2; // Not valid.
        it.value() += 2;
    }
    
    // {d, 6} {a, 3} {e, 7} {c, 5}
    for(const auto& key_value : map) {
        std::cout << "{" << key_value.first << ", " << key_value.second << "}" << std::endl;
    }
    
    
    
    
    /*
     * Calculating the hash and comparing two std::string may be slow. 
     * We can store the hash of each std::string in the hash map to make 
     * the inserts and lookups faster by setting StoreHash to true.
     */ 
    tsl::hopscotch_map<std::string, int, std::hash<std::string>, 
                       std::equal_to<std::string>,
                       std::allocator<std::pair<std::string, int>>,
                       30, true> map2;
                       
    map2["a"] = 1;
    map2["b"] = 2;
    
    // {a, 1} {b, 2}
    for(const auto& key_value : map2) {
        std::cout << "{" << key_value.first << ", " << key_value.second << "}" << std::endl;
    }
    
    
    
    
    tsl::hopscotch_set<int> set;
    set.insert({1, 9, 0});
    set.insert({2, -1, 9});
    
    // {0} {1} {2} {9} {-1}
    for(const auto& key : set) {
        std::cout << "{" << key << "}" << std::endl;
    }
} 
```

#### Heterogeneous lookups

Heterogeneous overloads allow the usage of other types than `Key` for lookup and erase operations as long as the used types are hashable and comparable to `Key`.

To activate the heterogeneous overloads in `tsl::hopscotch_map/set`, the qualified-id `KeyEqual::is_transparent` must be valid. It works the same way as for [`std::map::find`](http://en.cppreference.com/w/cpp/container/map/find). You can either use [`std::equal_to<>`](http://en.cppreference.com/w/cpp/utility/functional/equal_to_void) or define your own function object.

Both `KeyEqual` and `Hash` will need to be able to deal with the different types.

```c++
#include <functional>
#include <iostream>
#include <string>
#include "hopscotch_map.h"


struct employee {
    employee(int id, std::string name) : m_id(id), m_name(std::move(name)) {
    }
    
    friend bool operator==(const employee& empl, int empl_id) {
        return empl.m_id == empl_id;
    }
    
    friend bool operator==(int empl_id, const employee& empl) {
        return empl_id == empl.m_id;
    }
    
    friend bool operator==(const employee& empl1, const employee& empl2) {
        return empl1.m_id == empl2.m_id;
    }
    
    
    int m_id;
    std::string m_name;
};

struct hash_employee {
    std::size_t operator()(const employee& empl) const {
        return std::hash<int>()(empl.m_id);
    }
    
    std::size_t operator()(int id) const {
        return std::hash<int>()(id);
    }
};

struct equal_employee {
    using is_transparent = void;
    
    bool operator()(const employee& empl, int empl_id) const {
        return empl.m_id == empl_id;
    }
    
    bool operator()(int empl_id, const employee& empl) const {
        return empl_id == empl.m_id;
    }
    
    bool operator()(const employee& empl1, const employee& empl2) const {
        return empl1.m_id == empl2.m_id;
    }
};


int main() {
    // Use std::equal_to<> which will automatically deduce and forward the parameters
    tsl::hopscotch_map<employee, int, hash_employee, std::equal_to<>> map; 
    map.insert({employee(1, "John Doe"), 2001});
    map.insert({employee(2, "Jane Doe"), 2002});
    map.insert({employee(3, "John Smith"), 2003});

    // John Smith 2003
    auto it = map.find(3);
    if(it != map.end()) {
        std::cout << it->first.m_name << " " << it->second << std::endl;
    }

    map.erase(1);



    // Use a custom KeyEqual which has an is_transparent member type
    tsl::hopscotch_map<employee, int, hash_employee, equal_employee> map2;
    map2.insert({employee(4, "Johnny Doe"), 2004});

    // 2004
    std::cout << map2.at(4) << std::endl;
} 
```

#### Deny of Service (DoS) attack
In addition to `tsl::hopscotch_map` and `tsl::hopscotch_set`, the library provides two more "secure" options: `tsl::hopscotch_sc_map` and `tsl::hopscotch_sc_set`. 

These two additions have a worst-case runtime of O(log n) for lookups and deletions and an amortized worst case of O(log n) for insertions (amortized due to the possibility of rehash which would be in O(n)). Even if the hash function maps all the elements to the same bucket, the O(log n) would still hold.

This provides a security against hash table Deny of Service attacks. 

To achieve this, the "secure" versions use a binary search tree for the overflown elements (see [implementation details](https://tessil.github.io/2016/08/29/hopscotch-hashing.html)) and thus need the elements to be `LessThanComparable`. An additional `Compare` template parameter is needed.

```c++
#include <chrono>
#include <cstdint>
#include <iostream>
#include "hopscotch_map.h"
#include "hopscotch_sc_map.h"

/*
 * Poor hash function which always returns 1 to simulate
 * a Deny of Service attack.
 */
struct dos_attack_simulation_hash {
    std::size_t operator()(int id) const {
        return 1;
    }
};

int main() {
    /*
     * Slow due to the hash function, insertions are done in O(n).
     */
    tsl::hopscotch_map<int, int, dos_attack_simulation_hash> map;
    
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0; i < 10000; i++) {
        map.insert({i, 0});
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    // 110 ms
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << duration.count() << " ms" << std::endl;
    
    
    
    
    /*
     * Faster. Even with the poor hash function, insertions end-up to
     * be O(log n) in average (and O(n) when a rehash occurs).
     */
    tsl::hopscotch_sc_map<int, int, dos_attack_simulation_hash> map_secure;
    
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i < 10000; i++) {
        map_secure.insert({i, 0});
    }
    end = std::chrono::high_resolution_clock::now();
    
    // 2 ms
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << duration.count() << " ms" << std::endl;
} 
```

### License

The code is licensed under the MIT license, see the [LICENSE file](LICENSE) for details.
