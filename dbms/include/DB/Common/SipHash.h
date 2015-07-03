#pragma once

/** SipHash - быстрая криптографическая хэш функция для коротких строк.
  * Взято отсюда: https://www.131002.net/siphash/
  *
  * Сделано два изменения:
  * - возвращает 128 бит, а не 64;
  * - сделано потоковой (можно вычислять по частям).
  *
  * На коротких строках (URL, поисковые фразы) более чем в 3 раза быстрее MD5 от OpenSSL.
  * (~ 700 МБ/сек., 15 млн. строк в секунду)
  */

#include <cstdint>
#include <cstddef>

#define ROTL(x,b) (u64)( ((x) << (b)) | ( (x) >> (64 - (b))) )

#define SIPROUND											\
	do 														\
	{														\
		v0 += v1; v1=ROTL(v1,13); v1 ^= v0; v0=ROTL(v0,32); \
		v2 += v3; v3=ROTL(v3,16); v3 ^= v2;					\
		v0 += v3; v3=ROTL(v3,21); v3 ^= v0;					\
		v2 += v1; v1=ROTL(v1,17); v1 ^= v2; v2=ROTL(v2,32); \
	} while(0)


class SipHash
{
private:
	typedef uint64_t u64;
	typedef uint8_t u8;

	/// Состояние.
	u64 v0;
	u64 v1;
	u64 v2;
	u64 v3;

	/// Сколько байт обработано.
	u64 cnt;

	/// Текущие 8 байт входных данных.
	union
	{
		u64 current_word;
		u8 current_bytes[8];
	};

	void finalize()
	{
		/// В последний свободный байт пишем остаток от деления длины на 256.
		current_bytes[7] = cnt;

		v3 ^= current_word;
		SIPROUND;
		SIPROUND;
		v0 ^= current_word;

		v2 ^= 0xff;
		SIPROUND;
		SIPROUND;
		SIPROUND;
		SIPROUND;
	}

public:
	/// Аргументы - seed.
	SipHash(u64 k0 = 0, u64 k1 = 0)
	{
		/// Инициализируем состояние некоторыми случайными байтами и seed-ом.
		v0 = 0x736f6d6570736575ULL ^ k0;
		v1 = 0x646f72616e646f6dULL ^ k1;
		v2 = 0x6c7967656e657261ULL ^ k0;
		v3 = 0x7465646279746573ULL ^ k1;

		cnt = 0;
		current_word = 0;
	}

	void update(const char * data, u64 size)
	{
		const char * end = data + size;

		/// Дообработаем остаток от предыдущего апдейта, если есть.
		if (cnt & 7)
		{
			while (cnt & 7 && data < end)
			{
				current_bytes[cnt & 7] = *data;
				++data;
				++cnt;
			}

			/// Если всё ещё не хватает байт до восьмибайтового слова.
			if (cnt & 7)
				return;

			v3 ^= current_word;
			SIPROUND;
			SIPROUND;
			v0 ^= current_word;
		}

		cnt += end - data;

		while (data + 8 <= end)
		{
			current_word = *reinterpret_cast<const u64 *>(data);

			v3 ^= current_word;
			SIPROUND;
			SIPROUND;
			v0 ^= current_word;

			data += 8;
		}

		/// Заполняем остаток, которого не хватает до восьмибайтового слова.
		current_word = 0;
		switch (end - data)
		{
			case 7: current_bytes[6] = data[6];
			case 6: current_bytes[5] = data[5];
			case 5: current_bytes[4] = data[4];
			case 4: current_bytes[3] = data[3];
			case 3: current_bytes[2] = data[2];
			case 2: current_bytes[1] = data[1];
			case 1: current_bytes[0] = data[0];
			case 0: break;
		}
	}

	/// Получить результат в некотором виде. Это можно сделать только один раз!

	void get128(char * out)
	{
		finalize();
		reinterpret_cast<u64 *>(out)[0] = v0 ^ v1;
		reinterpret_cast<u64 *>(out)[1] = v2 ^ v3;
	}

	void get128(u64 & lo, u64 & hi)
	{
		finalize();
		lo = v0 ^ v1;
		hi = v2 ^ v3;
	}

	u64 get64()
	{
		finalize();
		return v0 ^ v1 ^ v2 ^ v3;
	}
};


#undef ROTL
#undef SIPROUND


inline void sipHash128(const char * data, const size_t size, char * out)
{
	SipHash hash;
	hash.update(data, size);
	hash.get128(out);
}

inline uint64_t sipHash64(const char * data, const size_t size)
{
	SipHash hash;
	hash.update(data, size);
	return hash.get64();
}

#include <string>

inline uint64_t sipHash64(const std::string & s)
{
	return sipHash64(s.data(), s.size());
}
