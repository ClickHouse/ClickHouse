# base-x [![License][license-img]][license-url] [![GitHub Stars][stars-img]][stars-url] [![GitHub Forks][forks-img]][forks-url] [![GitHub Watchers][watchers-img]][watchers-url] [![Tweet][tweet-img]][tweet-url]

[![Build Status](https://travis-ci.org/Kronuz/base-x.svg?branch=master)](https://travis-ci.org/Kronuz/base-x)


### BaseX encoder / decoder for C++

This is a fast base encoder / decoder of any given alphabet.


#### Example

``` cpp
// example.cc
// g++ -std=c++14 -o example example.cc

#include <iostream>
#include "base_x.hh"

int main() {
    auto encoded = Base58::base58().encode("Hello world!");

    std::cout << encoded << std::endl;
    // => 1LDlk6QWOejX6rPrJ

    return 0;
}
```


#### Compilation

* g++ and clang++ are supported.
* C++14 is required.


### Alphabets

See below for a list of commonly recognized alphabets, and their respective base.

Base | Factory             | Alphabet
-----|---------------------|-------------
   2 | base2::base2()      | `01`
   2 | base8::base8()      | `01234567`
  11 | bas11::bas11()      | `0123456789a`
  16 | base16::base16()    | `0123456789abcdef`
  32 | base32::base32()    | `0123456789ABCDEFGHJKMNPQRSTVWXYZ`
  36 | base36::base36()    | `0123456789abcdefghijklmnopqrstuvwxyz`
  58 | base58::base58()    | `123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz`
  58 | base58::bitcoin()   | `123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz`
  58 | base58::gmp()       | `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuv`
  58 | base58::ripple()    | `rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz`
  58 | base58::flickr()    | `123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ`
  62 | base62::base62()    | `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`
  62 | base62::inverted()  | `0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`
  64 | base64::base64()    | `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`
  64 | base64::urlsafe()   | `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_`
  66 | base66::base66()    | `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~`


### How it works

It encodes octet arrays by doing long divisions on all significant digits in the
array, creating a representation of that number in the new base.

**If you need standard hex encoding, or base64 encoding, this module is NOT
appropriate.**


## Author
[**German Mendez Bravo (Kronuz)**](https://kronuz.io/)

[![Follow on GitHub][github-follow-img]][github-follow-url]
[![Follow on Twitter][twitter-follow-img]][twitter-follow-url]


## License

MIT License. See [LICENSE](LICENSE) for details.

Copyright (c) 2017 German Mendez Bravo (Kronuz) @ german dot mb at gmail.com


[license-url]: https://github.com/Kronuz/base-x/blob/master/LICENSE
[license-img]: https://img.shields.io/github/license/Kronuz/base-x.svg
[stars-url]: https://github.com/Kronuz/base-x/stargazers
[stars-img]: https://img.shields.io/github/stars/Kronuz/base-x.svg?style=social&amp;label=Stars
[forks-url]: https://github.com/Kronuz/base-x/network/members
[forks-img]: https://img.shields.io/github/forks/Kronuz/base-x.svg?style=social&amp;label=Forks
[watchers-url]: https://github.com/Kronuz/base-x/watchers
[watchers-img]: https://img.shields.io/github/watchers/Kronuz/base-x.svg?style=social&amp;label=Watchers
[tweet-img]: https://img.shields.io/twitter/url/https/github.com/Kronuz/base-x.svg?style=social
[tweet-url]: https://twitter.com/intent/tweet?text=Base-X+encoding%2Fdecoding+for+modern+C%2B%2B+by+%40germbravo:&url=https%3A%2F%2Fgithub.com%2FKronuz%2Fbase-x
[github-follow-url]: https://github.com/Kronuz
[github-follow-img]: https://img.shields.io/github/followers/Kronuz.svg?style=social&label=Follow
[twitter-follow-url]: https://twitter.com/intent/follow?screen_name=germbravo
[twitter-follow-img]: https://img.shields.io/twitter/follow/germbravo.svg?style=social&label=Follow
