#pragma once

#include <iostream>
#include <iomanip>
#include <cstring>
#include <cstdint>
#include <sstream>
#include <fstream>
#include <stdexcept>

//Class mostly implemented by ChatGPT
class MD5 {
private:
	uint32_t state[4], count[2];
	uint8_t buffer[64];

	void update(const uint8_t* input, size_t length) {
		size_t index = (count[0] >> 3) & 0x3F;
		count[0] += static_cast<uint32_t>(length << 3);
		if (count[0] < (length << 3)) {
			count[1]++;
		}
		count[1] += static_cast<uint32_t>(length >> 29);

		size_t part_len = 64 - index, i = 0;

		if (length >= part_len) {
			std::memcpy(&buffer[index], input, part_len);
			transform(buffer);

			for (i = part_len; i + 63 < length; i += 64) {
				transform(&input[i]);
			}
			index = 0;
		} else {
			i = 0;
		}

		std::memcpy(&buffer[index], &input[i], length - i);
	}

	void finalize(uint8_t digest[16]) {
		uint8_t bits[8];
		encode(bits, count, 8);

		size_t index = (count[0] >> 3) & 0x3F, pad_len = (index < 56) ? (56 - index) : (120 - index);

		static const uint8_t PADDING[64] = { 0x80 };
		update(PADDING, pad_len);
		update(bits, 8);

		encode(digest, state, 16);
		reset();
	}

	void reset() {
		count[0] = 0;
		count[1] = 0;

		state[0] = 0x67452301;
		state[1] = 0xEFCDAB89;
		state[2] = 0x98BADCFE;
		state[3] = 0x10325476;
	}

	static uint32_t F(uint32_t x, uint32_t y, uint32_t z) {
		return (x & y) | (~x & z);
	}

	static uint32_t G(uint32_t x, uint32_t y, uint32_t z) {
		return (x & z) | (y & ~z);
	}

	static uint32_t H(uint32_t x, uint32_t y, uint32_t z) {
		return x ^ y ^ z;
	}

	static uint32_t I(uint32_t x, uint32_t y, uint32_t z) {
		return y ^ (x | ~z);
	}

	static void rotateLeft(uint32_t& x, int n) {
		x = (x << n) | (x >> (32 - n));
	}

	void transform(const uint8_t block[64]) {
		uint32_t a = state[0], b = state[1], c = state[2], d = state[3], x[16];
		decode(x, block, 64);

		// Constants for MD5
		static const uint32_t K[] = {
			0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
			0xf57c0faf, 0x4787c62a, 0xa8304613, 0xb00327c8,
			0xbf597fc7, 0x298e09bb, 0x0fb0a8c4, 0x20004157,
			0x0c1d9f8e, 0x40a38883, 0x2e2a3f3d, 0xe326b0d1,
			0x8b44f7a4, 0x3503b4bc, 0x0167b9b8, 0x059a3e69,
			0x0a8b75a0, 0x1887ca0b, 0x09ef3e0e, 0x3ed04363,
			0x1f29f2b6, 0x6f0c119e, 0x65d6f7c9, 0x76b232a2,
			0x1a66b185, 0x1134e232, 0x18f4af85, 0x2b0c7f84,
			0x64c89bc5, 0x96f0eb77, 0x5bd6aa88, 0x9e80a101,
			0x78d485ba, 0x00ae8e38, 0x5fda184e, 0x0c372f71,
			0x0da006f5, 0x90a1e9bc, 0xa62632e7, 0x4fcabc2a,
			0x304c3c5f, 0x079c3da5, 0x3d5c6f34, 0x5f02f4bc
		};
		static const int S[64] = {
			7, 12, 17, 22,  // Round 1
			5, 9, 14, 20,  // Round 2
			4, 11, 16, 23, // Round 3
			6, 10, 15, 21  // Round 4
		};

		// MD5 transformation steps
		for (int i = 0; i < 64; i++) {
			uint32_t f, g;
			if (i < 16) {
				f = F(b, c, d);
				g = i;
			} else if (i < 32) {
				f = G(b, c, d);
				g = (5 * i + 1) % 16;
			} else if (i < 48) {
				f = H(b, c, d);
				g = (3 * i + 5) % 16;
			} else {
				f = I(b, c, d);
				g = (7 * i) % 16;
			}

			f += a + K[i] + x[g];
			rotateLeft(f, S[i]);
			f += b;

			a = d;
			d = c;
			c = b;
			b = f;
		}

		state[0] += a;
		state[1] += b;
		state[2] += c;
		state[3] += d;
	}

	void encode(uint8_t* output, const uint32_t* input, size_t length) {
		for (size_t i = 0; i < length / 4; i++) {
			output[i * 4] = static_cast<uint8_t>(input[i] & 0xFF);
			output[i * 4 + 1] = static_cast<uint8_t>((input[i] >> 8) & 0xFF);
			output[i * 4 + 2] = static_cast<uint8_t>((input[i] >> 16) & 0xFF);
			output[i * 4 + 3] = static_cast<uint8_t>((input[i] >> 24) & 0xFF);
		}
	}

	void decode(uint32_t* output, const uint8_t* input, size_t length) {
		for (size_t i = 0; i < length / 4; i++) {
			output[i] = static_cast<uint32_t>(input[i * 4]) |
						(static_cast<uint32_t>(input[i * 4 + 1]) << 8) |
						(static_cast<uint32_t>(input[i * 4 + 2]) << 16) |
						(static_cast<uint32_t>(input[i * 4 + 3]) << 24);
		}
	}

public:
	void hashFile(const std::string& file_path, uint8_t digest[16]) {
		const size_t buffer_size = 8192; // 8 KB buffer
		uint8_t buffer[buffer_size];
		std::ifstream file(file_path, std::ios::binary);

		if (!file) {
			throw std::runtime_error("Could not open file: " + file_path);
		}

		this->reset();
		while (file.read(reinterpret_cast<char*>(buffer), buffer_size) || file.gcount() > 0) {
			this->update(buffer, file.gcount());
		}
		this->finalize(digest);
	}
};
