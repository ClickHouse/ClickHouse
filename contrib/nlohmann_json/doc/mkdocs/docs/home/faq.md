# Frequently Asked Questions (FAQ)

## Limitations

### Relaxed parsing

!!! question

	- Can you add an option to ignore trailing commas?

For the same reason this library does not support [comments](#comments), this library also does not support any feature which would jeopardize interoperability.


### Parse errors reading non-ASCII characters

!!! question "Questions"

	- Why is the parser complaining about a Chinese character?
	- Does the library support Unicode?
	- I get an exception `[json.exception.parse_error.101] parse error at line 1, column 53: syntax error while parsing value - invalid string: ill-formed UTF-8 byte; last read: '"Testé$')"`

The library supports **Unicode input** as follows:

- Only **UTF-8** encoded input is supported which is the default encoding for JSON according to [RFC 8259](https://tools.ietf.org/html/rfc8259.html#section-8.1).
- `std::u16string` and `std::u32string` can be parsed, assuming UTF-16 and UTF-32 encoding, respectively. These encodings are not supported when reading from files or other input containers.
- Other encodings such as Latin-1 or ISO 8859-1 are **not** supported and will yield parse or serialization errors.
- [Unicode noncharacters](http://www.unicode.org/faq/private_use.html#nonchar1) will not be replaced by the library.
- Invalid surrogates (e.g., incomplete pairs such as `\uDEAD`) will yield parse errors.
- The strings stored in the library are UTF-8 encoded. When using the default string type (`std::string`), note that its length/size functions return the number of stored bytes rather than the number of characters or glyphs.
- When you store strings with different encodings in the library, calling [`dump()`](https://nlohmann.github.io/json/classnlohmann_1_1basic__json_a50ec80b02d0f3f51130d4abb5d1cfdc5.html#a50ec80b02d0f3f51130d4abb5d1cfdc5) may throw an exception unless `json::error_handler_t::replace` or `json::error_handler_t::ignore` are used as error handlers.

In most cases, the parser is right to complain, because the input is not UTF-8 encoded. This is especially true for Microsoft Windows where Latin-1 or ISO 8859-1 is often the standard encoding.


### Key name in exceptions

!!! question

	Can I get the key of the object item that caused an exception?

No, this is not possible. See <https://github.com/nlohmann/json/issues/932> for a longer discussion.


## Serialization issues


### Number precision

!!! question

	- It seems that precision is lost when serializing a double.
	- Can I change the precision for floating-point serialization?

The library uses `std::numeric_limits<number_float_t>::digits10` (15 for IEEE `double`s) digits for serialization. This value is sufficient to guarantee roundtripping. If one uses more than this number of digits of precision, then string -> value -> string is not guaranteed to round-trip.

!!! quote "[cppreference.com](https://en.cppreference.com/w/cpp/types/numeric_limits/digits10)"

	The value of `std::numeric_limits<T>::digits10` is the number of base-10 digits that can be represented by the type T without change, that is, any number with this many significant decimal digits can be converted to a value of type T and back to decimal form, without change due to rounding or overflow. 

!!! tip

	The website https://float.exposed gives a good insight into the internal storage of floating-point numbers.


## Compilation issues

### Android SDK

!!! question

	Why does the code not compile with Android SDK?

Android defaults to using very old compilers and C++ libraries. To fix this, add the following to your `Application.mk`. This will switch to the LLVM C++ library, the Clang compiler, and enable C++11 and other features disabled by default.

```ini
APP_STL := c++_shared
NDK_TOOLCHAIN_VERSION := clang3.6
APP_CPPFLAGS += -frtti -fexceptions
```

The code compiles successfully with [Android NDK](https://developer.android.com/ndk/index.html?hl=ml), Revision 9 - 11 (and possibly later) and [CrystaX's Android NDK](https://www.crystax.net/en/android/ndk) version 10.


### Missing STL function

!!! question "Questions"

	- Why do I get a compilation error `'to_string' is not a member of 'std'` (or similarly, for `strtod` or `strtof`)?
	- Why does the code not compile with MinGW or Android SDK?

This is not an issue with the code,  but rather with the compiler itself. On Android, see above to build with a newer environment.  For MinGW, please refer to [this site](http://tehsausage.com/mingw-to-string) and [this discussion](https://github.com/nlohmann/json/issues/136) for information on how to fix this bug. For Android NDK using `APP_STL := gnustl_static`, please refer to [this discussion](https://github.com/nlohmann/json/issues/219).
