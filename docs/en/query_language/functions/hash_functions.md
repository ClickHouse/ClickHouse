# Hash functions

Hash functions can be used for deterministic pseudo-random shuffling of elements.

## halfMD5

Calculates the MD5 from a string. Then it takes the first 8 bytes of the hash and interprets them as UInt64 in big endian.
Accepts a String-type argument. Returns UInt64.
This function works fairly slowly (5 million short strings per second per processor core).
If you don't need MD5 in particular, use the 'sipHash64' function instead.

## MD5

Calculates the MD5 from a string and returns the resulting set of bytes as FixedString(16).
If you don't need MD5 in particular, but you need a decent cryptographic 128-bit hash, use the 'sipHash128' function instead.
If you want to get the same result as output by the md5sum utility, use lower(hex(MD5(s))).

## sipHash64

Calculates SipHash from a string.
Accepts a String-type argument. Returns UInt64.
SipHash is a cryptographic hash function. It works at least three times faster than MD5.
For more information, see the link: <https://131002.net/siphash/>

## sipHash128

Calculates SipHash from a string.
Accepts a String-type argument. Returns FixedString(16).
Differs from sipHash64 in that the final xor-folding state is only done up to 128 bytes.

## cityHash64

Calculates CityHash64 from a string or a similar hash function for any number of any type of arguments.
For String-type arguments, CityHash is used. This is a fast non-cryptographic hash function for strings with decent quality.
For other types of arguments, a decent implementation-specific fast non-cryptographic hash function is used.
If multiple arguments are passed, the function is calculated using the same rules and chain combinations using the CityHash combinator.
For example, you can compute the checksum of an entire table with accuracy up to the row order: `SELECT sum(cityHash64(*)) FROM table`.

## intHash32

Calculates a 32-bit hash code from any type of integer.
This is a relatively fast non-cryptographic hash function of average quality for numbers.

## intHash64

Calculates a 64-bit hash code from any type of integer.
It works faster than intHash32. Average quality.

## SHA1

## SHA224

## SHA256

Calculates SHA-1, SHA-224, or SHA-256 from a string and returns the resulting set of bytes as FixedString(20), FixedString(28), or FixedString(32).
The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).
We recommend using this function only in cases when you need a specific hash function and you can't select it.
Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in SELECTS.

## URLHash(url\[, N\])

A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` or `#` at the end, if present.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` or `#` at the end, if present.
Levels are the same as in URLHierarchy. This function is specific to Yandex.Metrica.

