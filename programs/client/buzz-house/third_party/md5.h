#pragma once

#include <iostream>
#include <iomanip>
#include <cstring>
#include <cstdint>
#include <sstream>
#include <fstream>
#include <stdexcept>

//Adapted from https://github.com/Zunawe/md5-c
class MD5 {
private:
	static const constexpr uint32_t S[] = {7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
										   5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20,
										   4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
										   6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21};

	static const constexpr uint32_t K[] = {0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
										   0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
										   0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
										   0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
										   0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
										   0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
										   0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
										   0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
										   0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
										   0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
										   0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
										   0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
										   0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
										   0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
										   0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
										   0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391};

	/*
	* Padding used to make the size (in bits) of the input congruent to 448 mod 512
	*/
	static const constexpr uint8_t PADDING[] = {0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
												0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	static const constexpr size_t in_buffer_size = 8192; // 8 KB buffer

	uint64_t isize;        // Size of input in bytes
	uint32_t abuffer[4];   // Current accumulation of hash
	uint8_t ninput[64];    // Input to be used in the next step
	uint8_t rdigest[16];   // Result of algorithm
	uint8_t ainput_buffer[in_buffer_size];

/*
* Rotates a 32-bit word left by n bits
*/
inline uint32_t rotateLeft(const uint32_t x, const uint32_t n) const {
	return (x << n) | (x >> (32 - n));
}

/*
 * Bit-manipulation functions defined by the MD5 algorithm
 */
inline uint32_t md5f(const uint32_t x, const uint32_t y, const uint32_t z) const {
	return ((x & y) | (~x & z));
}

inline uint32_t md5g(const uint32_t x, const uint32_t y, const uint32_t z) const {
	return ((x & z) | (y & ~z));
}

inline uint32_t md5h(const uint32_t x, const uint32_t y, const uint32_t z) const {
	return (x ^ y ^ z);
}

inline uint32_t md5i(const uint32_t x, const uint32_t y, const uint32_t z) const {
	return (y ^ (x | ~z));
}

/*
* Initialize a context
*/
void md5Init() {
	isize = UINT64_C(0);

	abuffer[0] = UINT64_C(0x67452301);
	abuffer[1] = UINT64_C(0xefcdab89);
	abuffer[2] = UINT64_C(0x98badcfe);
	abuffer[3] = UINT64_C(0x10325476);
}

/*
* Add some amount of input to the context
*
* If the input fills out a block of 512 bits, apply the algorithm (md5Step)
* and save the result in the buffer. Also updates the overall size.
*/
void md5Update(const uint8_t *input_buffer, const size_t input_len) {
	uint32_t input[16];
	unsigned int offset = isize % 64;

	isize += static_cast<uint64_t>(input_len);
	// Copy each byte in input_buffer into the next space in our context input
	for (unsigned int i = 0; i < input_len; ++i) {
		ninput[offset++] = static_cast<uint8_t>(*(input_buffer + i));

		// If we've filled our context input, copy it into our local array input
		// then reset the offset to 0 and fill in a new buffer.
		// Every time we fill out a chunk, we run it through the algorithm
		// to enable some back and forth between cpu and i/o
		if (offset % 64 == 0) {
			for (unsigned int j = 0; j < 16; ++j) {
				// Convert to little-endian
				// The local variable `input` our 512-bit chunk separated into 32-bit words
				// we can use in calculations
				input[j] = static_cast<uint32_t>(ninput[(j * 4) + 3]) << 24 |
						static_cast<uint32_t>(ninput[(j * 4) + 2]) << 16 |
						static_cast<uint32_t>(ninput[(j * 4) + 1]) <<  8 |
						static_cast<uint32_t>(ninput[(j * 4)]);
			}
			md5Step(abuffer, input);
			offset = 0;
		}
	}
}

/*
* Pad the current input to get to 448 bytes, append the size in bits to the very end,
* and save the result of the final iteration into digest.
*/
void md5Finalize() {
	uint32_t input[16];
	unsigned int offset = isize % 64, padding_length = offset < 56 ? 56 - offset : (56 + 64) - offset;

	// Fill in the padding and undo the changes to size that resulted from the update
	md5Update(PADDING, padding_length);
	isize -= static_cast<uint64_t>(padding_length);

	// Do a final update (internal to this function)
	// Last two 32-bit words are the two halves of the size (converted from bytes to bits)
	for (unsigned int j = 0; j < 14; ++j) {
		input[j] = static_cast<uint32_t>(ninput[(j * 4) + 3]) << 24 |
				   static_cast<uint32_t>(ninput[(j * 4) + 2]) << 16 |
				   static_cast<uint32_t>(ninput[(j * 4) + 1]) <<  8 |
				   static_cast<uint32_t>(ninput[(j * 4)]);
	}
	input[14] = static_cast<uint32_t>(isize * 8);
	input[15] = static_cast<uint32_t>((isize * 8) >> 32);

	md5Step(abuffer, input);

	// Move the result into digest (convert from little-endian)
	for (unsigned int i = 0; i < 4; ++i) {
		rdigest[(i * 4) + 0] = static_cast<uint8_t>((abuffer[i] & 0x000000FF));
		rdigest[(i * 4) + 1] = static_cast<uint8_t>((abuffer[i] & 0x0000FF00) >>  8);
		rdigest[(i * 4) + 2] = static_cast<uint8_t>((abuffer[i] & 0x00FF0000) >> 16);
		rdigest[(i * 4) + 3] = static_cast<uint8_t>((abuffer[i] & 0xFF000000) >> 24);
	}
}

/*
* Step on 512 bits of input with the main MD5 algorithm.
*/
void md5Step(uint32_t *buffer, uint32_t *input) {
	uint32_t AA = buffer[0], BB = buffer[1], CC = buffer[2], DD = buffer[3], E;
	unsigned int j;

	for (unsigned int i = 0; i < 64; ++i) {
		switch (i / 16) {
			case 0:
				E = md5f(BB, CC, DD);
				j = i;
				break;
			case 1:
				E = md5g(BB, CC, DD);
				j = ((i * 5) + 1) % 16;
				break;
			case 2:
				E = md5h(BB, CC, DD);
				j = ((i * 3) + 5) % 16;
				break;
			default:
				E = md5i(BB, CC, DD);
				j = (i * 7) % 16;
				break;
		}

		uint32_t temp = DD;
		DD = CC;
		CC = BB;
		BB = BB + rotateLeft(AA + E + K[i] + input[j], S[i]);
		AA = temp;
	}

	buffer[0] += AA;
	buffer[1] += BB;
	buffer[2] += CC;
	buffer[3] += DD;
}

public:
	void hashFile(const std::string& file_path, uint8_t digest[16]) {
		std::ifstream file(file_path, std::ios::binary);

		if (!file) {
			throw std::runtime_error("Could not open file: " + file_path);
		}

		md5Init();
		while (file.read(reinterpret_cast<char*>(ainput_buffer), in_buffer_size) || file.gcount() > 0) {
			md5Update(ainput_buffer, file.gcount());
		}
		md5Finalize();
		std::memcpy(digest, rdigest, sizeof(uint8_t) * 16);
	}
};
