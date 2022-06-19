/*
base_x.hh
BaseX encoder / decoder for C++

Copyright (c) 2017 German Mendez Bravo (Kronuz) @ german dot mb at gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#ifndef __BASE_X__H_
#define __BASE_X__H_

#include <algorithm>        // for std::find_if, std::reverse
#include <stdexcept>        // for std::invalid_argument
#include <string>           // for std::string
#include <type_traits>      // for std::enable_if_t

#include "uinteger_t.hh"


class BaseX {
	char _chr[256];
	int _ord[256];

	const int size;
	const int alphabet_base;
	const unsigned base_size;
	const unsigned alphabet_base_bits;
	const unsigned block_size;
	const uinteger_t::digit alphabet_base_mask;
	const unsigned padding_size;
	const char padding;
	const int flags;

	constexpr char chr(unsigned char ord) const {
		return _chr[ord];
	}

	constexpr int ord(unsigned char chr) const {
		return _ord[chr];
	}

public:
	static constexpr int ignore_case =   (1 << 0);
	static constexpr int with_checksum = (1 << 1);
	static constexpr int with_check =    (1 << 2);
	static constexpr int block_padding = (1 << 3);

	template <std::size_t alphabet_size1, std::size_t extended_size1, std::size_t padding_size1, std::size_t translate_size1>
	constexpr BaseX(int flgs, const char (&alphabet)[alphabet_size1], const char (&extended)[extended_size1], const char (&padding_string)[padding_size1], const char (&translate)[translate_size1]) :
		_chr(),
		_ord(),
		size(alphabet_size1 - 1 + extended_size1 - 1),
		alphabet_base(alphabet_size1 - 1),
		base_size(uinteger_t::base_size(alphabet_base)),
		alphabet_base_bits(uinteger_t::base_bits(alphabet_base)),
		block_size((flgs & BaseX::block_padding) ? alphabet_base_bits : 0),
		alphabet_base_mask(alphabet_base - 1),
		padding_size(padding_size1 - 1),
		padding(padding_size ? padding_string[0] : '\0'),
		flags(flgs)
	{
		for (int c = 0; c < 256; ++c) {
			_chr[c] = 0;
			_ord[c] = alphabet_base;
		}
		for (int cp = 0; cp < alphabet_base; ++cp) {
			auto ch = alphabet[cp];
			_chr[cp] = ch;
			ASSERT(_ord[(unsigned char)ch] == alphabet_base);  // Duplicate character in the alphabet
			_ord[(unsigned char)ch] = cp;
			if (flags & BaseX::ignore_case) {
				if (ch >= 'A' && ch <='Z') {
					_ord[(unsigned char)ch - 'A' + 'a'] = cp;
				} else if (ch >= 'a' && ch <='z') {
					_ord[(unsigned char)ch - 'a' + 'A'] = cp;
				}
			}
		}
		for (std::size_t i = 0; i < extended_size1 - 1; ++i) {
			auto ch = extended[i];
			auto cp = alphabet_base + i;
			_chr[cp] = ch;
			ASSERT(_ord[(unsigned char)ch] == alphabet_base); // Duplicate character in the extended alphabet
			_ord[(unsigned char)ch] = cp;
			if (flags & BaseX::ignore_case) {
				if (ch >= 'A' && ch <='Z') {
					_ord[(unsigned char)ch - 'A' + 'a'] = cp;
				} else if (ch >= 'a' && ch <='z') {
					_ord[(unsigned char)ch - 'a' + 'A'] = cp;
				}
			}
		}
		int cp = -1;
		for (std::size_t i = 0; i < translate_size1 - 1; ++i) {
			auto ch = translate[i];
			auto ncp = _ord[(unsigned char)ch];
			if (ncp >= alphabet_base) {
				ASSERT(_ord[(unsigned char)ch] == alphabet_base); // Invalid translation character
				_ord[(unsigned char)ch] = cp;
				if (flags & BaseX::ignore_case) {
					if (ch >= 'A' && ch <='Z') {
						_ord[(unsigned char)ch - 'A' + 'a'] = cp;
					} else if (ch >= 'a' && ch <='z') {
						_ord[(unsigned char)ch - 'a' + 'A'] = cp;
					}
				}
			} else {
				cp = ncp;
			}
		}
	}

	// Get string representation of value
	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void encode(Result& result, const uinteger_t& input) const {
		std::size_t bp = 0;
		uinteger_t quotient;
		if (block_size) {
			bp = ((input.bits() + 7) & 0xf8) % block_size;
			bp = bp ? (block_size - bp) % block_size : 0;
			if (bp) {
				quotient = input << bp;
			}
		}
		const uinteger_t& num = bp ? quotient : input;
		auto num_sz = num.size();
		if (num_sz) {
			int sum = 0;
			result.reserve(num_sz * base_size);
			if (alphabet_base_bits) {
				std::size_t shift = 0;
				auto ptr = reinterpret_cast<const uinteger_t::half_digit*>(num.data());
				uinteger_t::digit v = *ptr++;
				v <<= uinteger_t::half_digit_bits;
				for (auto i = num_sz * 2 - 1; i; --i) {
					v >>= uinteger_t::half_digit_bits;
					v |= (static_cast<uinteger_t::digit>(*ptr++) << uinteger_t::half_digit_bits);
					do {
						auto d = static_cast<int>((v >> shift) & alphabet_base_mask);
						result.push_back(chr(d));
						shift += alphabet_base_bits;
						sum += d;
					} while (shift <= uinteger_t::half_digit_bits);
					shift -= uinteger_t::half_digit_bits;
				}
				v >>= (shift + uinteger_t::half_digit_bits);
				while (v) {
					auto d = static_cast<int>(v & alphabet_base_mask);
					result.push_back(chr(d));
					v >>= alphabet_base_bits;
					sum += d;
				}
				auto s = chr(0);
				auto rit_f = std::find_if(result.rbegin(), result.rend(), [s](const char& c) { return c != s; });
				result.resize(result.rend() - rit_f); // shrink
			} else {
				uinteger_t uint_base = alphabet_base;
				if (!bp) {
					quotient = num;
				}
				do {
					auto r = quotient.divmod(uint_base);
					auto d = static_cast<int>(r.second);
					result.push_back(chr(d));
					quotient = std::move(r.first);
					sum += d;
				} while (quotient);
			}
			std::reverse(result.begin(), result.end());
			if (padding_size) {
				Result p;
				p.resize((padding_size - (result.size() % padding_size)) % padding_size, padding);
				result.append(p);
			}
			if (flags & BaseX::with_check) {
				auto chk = static_cast<int>(num % size);
				result.push_back(chr(chk));
				sum += chk;
			}
			if (flags & BaseX::with_checksum) {
				auto sz = result.size();
				sz = (sz + sz / size) % size;
				sum += sz;
				sum = (size - sum % size) % size;
				result.push_back(chr(sum));
			}
		} else {
			result.push_back(chr(0));
		}
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result encode(const uinteger_t& num) const {
		Result result;
		encode(result, num);
		return result;
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void encode(Result& result, const unsigned char* decoded, std::size_t decoded_size) const {
		encode(result, uinteger_t(decoded, decoded_size, 256));
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result encode(const unsigned char* decoded, std::size_t decoded_size) const {
		Result result;
		encode(result, uinteger_t(decoded, decoded_size, 256));
		return result;
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void encode(Result& result, const char* decoded, std::size_t decoded_size) const {
		encode(result, uinteger_t(decoded, decoded_size, 256));
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result encode(const char* decoded, std::size_t decoded_size) const {
		Result result;
		encode(result, uinteger_t(decoded, decoded_size, 256));
		return result;
	}

	template <typename Result = std::string, typename T, std::size_t N, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void encode(Result& result, T (&s)[N]) const {
		encode(result, s, N - 1);
	}

	template <typename Result = std::string, typename T, std::size_t N, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result encode(T (&s)[N]) const {
		Result result;
		encode(result, s, N - 1);
		return result;
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void encode(Result& result, const std::string& binary) const {
		return encode(result, binary.data(), binary.size());
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value>>
	Result encode(const std::string& binary) const {
		Result result;
		encode(result, binary.data(), binary.size());
		return result;
	}

	void decode(uinteger_t& result, const char* encoded, std::size_t encoded_size) const {
		result = 0;
		int sum = 0;
		int sumsz = 0;
		int direction = 1;

		auto sz = encoded_size;
		if (flags & BaseX::with_checksum) --sz;
		if (flags & BaseX::with_check) --sz;

		int bp = 0;

		if (alphabet_base_bits) {
			for (; sz; --sz, encoded += direction) {
				auto c = *encoded;
				if (c == padding) break;
				auto d = ord(static_cast<int>(c));
				if (d < 0) continue; // ignored character
				if (d >= alphabet_base) {
					throw std::invalid_argument("Error: Invalid character: '" + std::string(1, c) + "' at " + std::to_string(encoded_size - sz));
				}
				sum += d;
				++sumsz;
				result = (result << alphabet_base_bits) | d;
				bp += block_size;
			}
		} else {
			uinteger_t uint_base = alphabet_base;
			for (; sz; --sz, encoded += direction) {
				auto c = *encoded;
				if (c == padding) break;
				auto d = ord(static_cast<int>(c));
				if (d < 0) continue; // ignored character
				if (d >= alphabet_base) {
					throw std::invalid_argument("Error: Invalid character: '" + std::string(1, c) + "' at " + std::to_string(encoded_size - sz));
				}
				sum += d;
				++sumsz;
				result = (result * uint_base) + d;
				bp += block_size;
			}
		}

		for (; sz && *encoded == padding; --sz, ++encoded);

		result >>= (bp & 7);

		if (flags & BaseX::with_check) {
			auto c = *encoded;
			auto d = ord(static_cast<int>(c));
			if (d < 0 || d >= size) {
				throw std::invalid_argument("Error: Invalid character: '" + std::string(1, c) + "' at " + std::to_string(encoded_size - sz));
			}
			auto chk = static_cast<int>(result % size);
			if (d != chk) {
				throw std::invalid_argument("Error: Invalid check");
			}
			sum += chk;
			++sumsz;
			++encoded;
		}

		if (flags & BaseX::with_checksum) {
			auto c = *encoded;
			auto d = ord(static_cast<int>(c));
			if (d < 0 || d >= size) {
				throw std::invalid_argument("Error: Invalid character: '" + std::string(1, c) + "' at " + std::to_string(encoded_size - sz));
			}
			sum += d;
			sum += (sumsz + sumsz / size) % size;
			if (sum % size) {
				throw std::invalid_argument("Error: Invalid checksum");
			}
		}
	}

	template <typename Result, typename = typename std::enable_if_t<uinteger_t::is_result<Result>::value>>
	void decode(Result& result, const char* encoded, std::size_t encoded_size) const {
		uinteger_t num;
		decode(num, encoded, encoded_size);
		result = num.template str<Result>(256);
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value or std::is_integral<Result>::value>>
	Result decode(const char* encoded, std::size_t encoded_size) const {
		Result result;
		decode(result, encoded, encoded_size);
		return result;
	}

	template <typename Result = std::string, typename T, std::size_t N, typename = std::enable_if_t<uinteger_t::is_result<Result>::value or std::is_integral<Result>::value>>
	void decode(Result& result, T (&s)[N]) const {
		decode(result, s, N - 1);
	}

	template <typename Result = std::string, typename T, std::size_t N, typename = std::enable_if_t<uinteger_t::is_result<Result>::value or std::is_integral<Result>::value>>
	Result decode(T (&s)[N]) const {
		Result result;
		decode(result, s, N - 1);
		return result;
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value or std::is_integral<Result>::value>>
	void decode(Result& result, const std::string& encoded) const {
		decode(result, encoded.data(), encoded.size());
	}

	template <typename Result = std::string, typename = std::enable_if_t<uinteger_t::is_result<Result>::value or std::is_integral<Result>::value>>
	Result decode(const std::string& encoded) const {
		Result result;
		decode(result, encoded.data(), encoded.size());
		return result;
	}

	bool is_valid(const char* encoded, std::size_t encoded_size) const {
		int sum = 0;
		int sumsz = 0;
		if (flags & BaseX::with_checksum) --sumsz;
		for (; encoded_size; --encoded_size, ++encoded) {
			auto d = ord(static_cast<int>(*encoded));
			if (d < 0) continue; // ignored character
			if (d >= alphabet_base) {
				return false;
			}
			sum += d;
			++sumsz;
		}
		if (flags & BaseX::with_checksum) {
			sum += (sumsz + sumsz / size) % size;
			if (sum % size) {
				return false;
			}
		}
		return true;
	}

	template <typename T, std::size_t N>
	bool is_valid(T (&s)[N]) const {
		return is_valid(s, N - 1);
	}

	bool is_valid(const std::string& encoded) const {
		return is_valid(encoded.data(), encoded.size());
	}
};

// base2
struct Base2 {
	static const BaseX& base2() {
		static constexpr BaseX encoder(0, "01", "", "", "");
		return encoder;
	}
	static const BaseX& base2chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "01", "", "", "");
		return encoder;
	}
};

// base8
struct Base8 {
	static const BaseX& base8() {
		static constexpr BaseX encoder(0, "01234567", "", "", "");
		return encoder;
	}
	static const BaseX& base8chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "01234567", "", "", "");
		return encoder;
	}
};

// base11
struct Base11 {
	static const BaseX& base11() {
		static constexpr BaseX encoder(BaseX::ignore_case, "0123456789a", "", "", "");
		return encoder;
	}
	static const BaseX& base11chk() {
		static constexpr BaseX encoder(BaseX::ignore_case | BaseX::with_checksum, "0123456789a", "", "", "");
		return encoder;
	}
};

// base16
struct Base16 {
	static const BaseX& base16() {
		static constexpr BaseX encoder(BaseX::ignore_case, "0123456789abcdef", "", "", "");
		return encoder;
	}
	static const BaseX& base16chk() {
		static constexpr BaseX encoder(BaseX::ignore_case | BaseX::with_checksum, "0123456789abcdef", "", "", "");
		return encoder;
	}
	static const BaseX& rfc4648() {
		static constexpr BaseX encoder(0, "0123456789ABCDEF", "", "", "");
		return encoder;
	}
};

// base32
struct Base32 {
	static const BaseX& base32() {
		static constexpr BaseX encoder(BaseX::ignore_case, "0123456789abcdefghijklmnopqrstuv", "", "", "");
		return encoder;
	}
	static const BaseX& base32chk() {
		static constexpr BaseX encoder(BaseX::ignore_case | BaseX::with_checksum, "0123456789abcdefghijklmnopqrstuv", "", "", "");
		return encoder;
	}
	static const BaseX& crockford() {
		static constexpr BaseX encoder(BaseX::ignore_case, "0123456789ABCDEFGHJKMNPQRSTVWXYZ", "", "", "-0O1IL");
		return encoder;
	}
	static const BaseX& crockfordchk() {
		static constexpr BaseX encoder(BaseX::ignore_case | BaseX::with_check, "0123456789ABCDEFGHJKMNPQRSTVWXYZ", "*~$=U", "", "-0O1IL");
		return encoder;
	}
	static const BaseX& rfc4648() {
		static constexpr BaseX encoder(BaseX::block_padding, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", "", "========", "\n\r");
		return encoder;
	}
	static const BaseX& rfc4648hex() {
		static constexpr BaseX encoder(BaseX::block_padding, "0123456789ABCDEFGHIJKLMNOPQRSTUV", "", "========", "\n\r");
		return encoder;
	}
};

// base36
struct Base36 {
	static const BaseX& base36() {
		static constexpr BaseX encoder(BaseX::ignore_case, "0123456789abcdefghijklmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
	static const BaseX& base36chk() {
		static constexpr BaseX encoder(BaseX::ignore_case | BaseX::with_checksum, "0123456789abcdefghijklmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
};

// base58
struct Base58 {
	static const BaseX& base58() {
		static constexpr BaseX encoder(0, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuv", "", "", "");
		return encoder;
	}
	static const BaseX& base58chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuv", "", "", "");
		return encoder;
	}
	static const BaseX& bitcoin() {
		static constexpr BaseX encoder(0, "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
	static const BaseX& bitcoinchk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
	static const BaseX& ripple() {
		static constexpr BaseX encoder(0, "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz", "", "", "");
		return encoder;
	}
	static const BaseX& ripplechk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz", "", "", "");
		return encoder;
	}
	static const BaseX& flickr() {
		static constexpr BaseX encoder(0, "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ", "", "", "");
		return encoder;
	}
	static const BaseX& flickrchk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ", "", "", "");
		return encoder;
	}
};

// base59
struct Base59 {
	static const BaseX& base59() {
		static constexpr BaseX encoder(0, "23456789abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ", "", "", "l1IO0");
		return encoder;
	}
	static const BaseX& base59chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "23456789abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ", "", "", "l1IO0");
		return encoder;
	}
	static const BaseX& dubaluchk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "zy9MalDxwpKLdvW2AtmscgbYUq6jhP7E53TiXenZRkVCrouBH4GSQf8FNJO", "", "", "-l1IO0");
		return encoder;
	}
};

// base62
struct Base62 {
	static const BaseX& base62() {
		static constexpr BaseX encoder(0, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
	static const BaseX& base62chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "", "", "");
		return encoder;
	}
	static const BaseX& inverted() {
		static constexpr BaseX encoder(0, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", "", "", "");
		return encoder;
	}
	static const BaseX& invertedchk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", "", "", "");
		return encoder;
	}
};

// base64
struct Base64 {
	static const BaseX& base64() {
		static constexpr BaseX encoder(0, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", "", "", "");
		return encoder;
	}
	static const BaseX& base64chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", "", "", "");
		return encoder;
	}
	static const BaseX& url() {
		static constexpr BaseX encoder(0, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", "", "", "");
		return encoder;
	}
	static const BaseX& urlchk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", "", "", "");
		return encoder;
	}
	static const BaseX& rfc4648() {
		static constexpr BaseX encoder(BaseX::block_padding, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", "", "====", "\n\r");
		return encoder;
	}
	static const BaseX& rfc4648url() {
		static constexpr BaseX encoder(BaseX::block_padding, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", "", "====", "\n\r");
		return encoder;
	}
};

// base66
struct Base66 {
	static const BaseX& base66() {
		static constexpr BaseX encoder(0, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~", "", "", "");
		return encoder;
	}
	static const BaseX& base66chk() {
		static constexpr BaseX encoder(BaseX::with_checksum, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~", "", "", "");
		return encoder;
	}
};

#endif
