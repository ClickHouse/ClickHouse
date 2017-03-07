#include <DB/Functions/FunctionsString.h>

#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArray.h>


namespace DB
{

  FunctionPtr FunctionReverse::create(const Context& context)
	{
		return std::make_shared<FunctionReverse>();
	}


  String FunctionReverse::getName() const
{
	return name;
}


DataTypePtr FunctionReverse::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0])
		&& !typeid_cast<const DataTypeArray *>(&*arguments[0]))
		throw Exception(
			"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return arguments[0]->clone();
}


void FunctionReverse::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
	if (const ColumnString * col = typeid_cast<const ColumnString *>(column.get()))
	{
		std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
		block.safeGetByPosition(result).column = col_res;
		ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(column.get()))
	{
		auto col_res = std::make_shared<ColumnFixedString>(col->getN());
		block.safeGetByPosition(result).column = col_res;
		ReverseImpl::vector_fixed(col->getChars(), col->getN(), col_res->getChars());
	}
	else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(column.get()))
	{
		String res;
		ReverseImpl::constant(col->getData(), res);
		auto col_res = std::make_shared<ColumnConstString>(col->size(), res);
		block.safeGetByPosition(result).column = col_res;
	}
	else if (typeid_cast<const ColumnArray *>(column.get()) || typeid_cast<const ColumnConstArray *>(column.get()))
	{
		FunctionArrayReverse().execute(block, arguments, result);
	}
	else
		throw Exception(
			"Illegal column " + block.safeGetByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
}


String FunctionAppendTrailingCharIfAbsent::getName() const
{
	return name;
}


DataTypePtr FunctionAppendTrailingCharIfAbsent::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
		throw Exception{
			"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

	if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
		throw Exception{
			"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

	return std::make_shared<DataTypeString>();
}


void FunctionAppendTrailingCharIfAbsent::executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
{
	const auto & column = block.safeGetByPosition(arguments[0]).column;
	const auto & column_char = block.safeGetByPosition(arguments[1]).column;

	if (!typeid_cast<const ColumnConstString *>(column_char.get()))
		throw Exception{"Second argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_COLUMN};

	const auto & trailing_char_str = static_cast<const ColumnConstString &>(*column_char).getData();

	if (trailing_char_str.size() != 1)
		throw Exception{"Second argument of function " + getName() + " must be a one-character string", ErrorCodes::BAD_ARGUMENTS};

	if (const auto col = typeid_cast<const ColumnString *>(&*column))
	{
		auto col_res = std::make_shared<ColumnString>();
		block.safeGetByPosition(result).column = col_res;

		const auto & src_data = col->getChars();
		const auto & src_offsets = col->getOffsets();

		auto & dst_data = col_res->getChars();
		auto & dst_offsets = col_res->getOffsets();

		const auto size = src_offsets.size();
		dst_data.resize(src_data.size() + size);
		dst_offsets.resize(size);

		ColumnString::Offset_t src_offset{};
		ColumnString::Offset_t dst_offset{};

		for (const auto i : ext::range(0, size))
		{
			const auto src_length = src_offsets[i] - src_offset;
			memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
			src_offset = src_offsets[i];
			dst_offset += src_length;

			if (src_length > 1 && dst_data[dst_offset - 2] != trailing_char_str.front())
			{
				dst_data[dst_offset - 1] = trailing_char_str.front();
				dst_data[dst_offset] = 0;
				++dst_offset;
			}

			dst_offsets[i] = dst_offset;
		}

		dst_data.resize_assume_reserved(dst_offset);
	}
	else if (const auto col = typeid_cast<const ColumnConstString *>(&*column))
	{
		const auto & in_data = col->getData();

		block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col->size(),
			in_data.size() == 0 ? in_data : in_data.back() == trailing_char_str.front() ? in_data : in_data + trailing_char_str);
	}
	else
		throw Exception{
			"Illegal column " + block.safeGetByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN};
}


void registerFunctionsString(FunctionFactory & factory)
{
	factory.registerFunction<FunctionEmpty>();
	factory.registerFunction<FunctionNotEmpty>();
	factory.registerFunction<FunctionLength>();
	factory.registerFunction<FunctionLengthUTF8>();
	factory.registerFunction<FunctionLower>();
	factory.registerFunction<FunctionUpper>();
	factory.registerFunction<FunctionLowerUTF8>();
	factory.registerFunction<FunctionUpperUTF8>();
	factory.registerFunction<FunctionReverse>();
	factory.registerFunction<FunctionReverseUTF8>();
	factory.registerFunction<FunctionConcat>();
	factory.registerFunction<FunctionConcatAssumeInjective>();
	factory.registerFunction<FunctionSubstring>();
	factory.registerFunction<FunctionSubstringUTF8>();
	factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
}

template <bool negative>
void EmptyImpl<negative>::vector(const ColumnString::Chars_t & data, const IColumn::Offsets_t & offsets, PaddedPODArray<UInt8> & res)
{
	size_t size = offsets.size();
	ColumnString::Offset_t prev_offset = 1;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = negative ^ (offsets[i] == prev_offset);
		prev_offset = offsets[i] + 1;
	}
}

template <bool negative>
void EmptyImpl<negative>::vector_fixed_to_constant(const ColumnString::Chars_t & data, size_t n, UInt8 & res)
{
	res = negative ^ (n == 0);
}

template <bool negative>
void EmptyImpl<negative>::constant(const std::__cxx11::string & data, UInt8 & res)
{
	res = negative ^ (data.empty());
}

template <bool negative>
void EmptyImpl<negative>::array(const IColumn::Offsets_t & offsets, PaddedPODArray<UInt8> & res)
{
	size_t size = offsets.size();
	ColumnString::Offset_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = negative ^ (offsets[i] == prev_offset);
		prev_offset = offsets[i];
	}
}

template <bool negative>
void EmptyImpl<negative>::constant_array(const Array & data, UInt8 & res)
{
	res = negative ^ (data.empty());
}

template <bool negative>
void EmptyImpl<negative>::vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt8> & res)
{
}


void LengthImpl::vector(const ColumnString::Chars_t & data, const IColumn::Offsets_t & offsets, PaddedPODArray<UInt64> & res)
{
	size_t size = offsets.size();
	for (size_t i = 0; i < size; ++i)
		res[i] = i == 0 ? (offsets[i] - 1) : (offsets[i] - 1 - offsets[i - 1]);
}

void LengthImpl::vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
{
}

void LengthImpl::constant(const std::__cxx11::string & data, UInt64 & res)
{
	res = data.size();
}

void LengthImpl::array(const IColumn::Offsets_t & offsets, PaddedPODArray<UInt64> & res)
{
	size_t size = offsets.size();
	for (size_t i = 0; i < size; ++i)
		res[i] = i == 0 ? (offsets[i]) : (offsets[i] - offsets[i - 1]);
}

void LengthImpl::constant_array(const Array & data, UInt64 & res)
{
	res = data.size();
}


void LengthUTF8Impl::vector(const ColumnString::Chars_t & data, const IColumn::Offsets_t & offsets, PaddedPODArray<UInt64> & res)
{
	size_t size = offsets.size();

	ColumnString::Offset_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = 0;
		for (const UInt8 * c = &data[prev_offset]; c + 1 < &data[offsets[i]]; ++c)
			if (*c <= 0x7F || *c >= 0xC0)
				++res[i];
		prev_offset = offsets[i];
	}
}

void LengthUTF8Impl::vector_fixed_to_constant(const ColumnString::Chars_t & data, size_t n, UInt64 & res)
{
}

void LengthUTF8Impl::vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
{
	size_t size = data.size() / n;

	for (size_t i = 0; i < size; ++i)
	{
		res[i] = 0;
		for (const UInt8 * c = &data[i * n]; c < &data[(i + 1) * n]; ++c)
			if (*c <= 0x7F || *c >= 0xC0)
				++res[i];
	}
}
void LengthUTF8Impl::constant(const std::__cxx11::string & data, UInt64 & res)
{
	res = 0;
	for (const UInt8 * c = reinterpret_cast<const UInt8 *>(data.data()); c < reinterpret_cast<const UInt8 *>(data.data() + data.size());
		 ++c)
		if (*c <= 0x7F || *c >= 0xC0)
			++res;
}

void LengthUTF8Impl::array(const IColumn::Offsets_t & offsets, PaddedPODArray<UInt64> & res)
{
	throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void LengthUTF8Impl::constant_array(const Array & data, UInt64 & res)
{
	throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


template <char not_case_lower_bound, char not_case_upper_bound>
void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.resize(data.size());
	res_offsets.assign(offsets);
	array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound, char not_case_upper_bound>
void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::constant(
	const std::__cxx11::string & data, std::__cxx11::string & res_data)
{
	res_data.resize(data.size());
	array(reinterpret_cast<const UInt8 *>(data.data()),
		reinterpret_cast<const UInt8 *>(data.data() + data.size()),
		reinterpret_cast<UInt8 *>(&res_data[0]));
}

template <char not_case_lower_bound, char not_case_upper_bound>
void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
	const auto flip_case_mask = 'A' ^ 'a';

#if __SSE2__
	const auto bytes_sse = sizeof(__m128i);
	const auto src_end_sse = src_end - (src_end - src) % bytes_sse;

	const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
	const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
	const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

	for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
	{
		/// load 16 sequential 8-bit characters
		const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

		/// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
		const auto is_not_case
			= _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

		/// keep `flip_case_mask` only where necessary, zero out elsewhere
		const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

		/// flip case by applying calculated mask
		const auto cased_chars = _mm_xor_si128(chars, xor_mask);

		/// store result back to destination
		_mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
	}
#endif

	for (; src < src_end; ++src, ++dst)
		if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
			*dst = *src ^ flip_case_mask;
		else
			*dst = *src;
}


template <char not_case_lower_bound, char not_case_upper_bound>
void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector_fixed(
	const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
{
	res_data.resize(data.size());
	array(data.data(), data.data() + data.size(), res_data.data());
}


template <bool to_lower>
void UTF8CyrillicToCase(const UInt8 *& src, const UInt8 * const src_end, UInt8 *& dst)
{
	if (src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
	{
		/// ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ
		*dst++ = xor_or_identity<to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<to_lower>(*src++, 0x10);
	}
	else if (src[0] == 0xD1u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
	{
		/// ѐёђѓєѕіїјљњћќѝўџ
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x10);
	}
	else if (src[0] == 0xD0u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
	{
		/// А-П
		*dst++ = *src++;
		*dst++ = xor_or_identity<to_lower>(*src++, 0x20);
	}
	else if (src[0] == 0xD0u && (src[1] >= 0xB0u && src[1] <= 0xBFu))
	{
		/// а-п
		*dst++ = *src++;
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
	}
	else if (src[0] == 0xD0u && (src[1] >= 0xA0u && src[1] <= 0xAFu))
	{
		///	Р-Я
		*dst++ = xor_or_identity<to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<to_lower>(*src++, 0x20);
	}
	else if (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
	{
		/// р-я
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
	}
}


template <char not_case_lower_bound,
	char not_case_upper_bound,
	int to_case(int),
	void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.resize(data.size());
	res_offsets.assign(offsets);
	array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
	char not_case_upper_bound,
	int to_case(int),
	void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector_fixed(
	const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
{
	res_data.resize(data.size());
	array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
	char not_case_upper_bound,
	int to_case(int),
	void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::constant(
	const std::__cxx11::string & data, std::__cxx11::string & res_data)
{
	res_data.resize(data.size());
	array(reinterpret_cast<const UInt8 *>(data.data()),
		reinterpret_cast<const UInt8 *>(data.data() + data.size()),
		reinterpret_cast<UInt8 *>(&res_data[0]));
}

template <char not_case_lower_bound,
	char not_case_upper_bound,
	int to_case(int),
	void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(
	const UInt8 *& src, const UInt8 * const src_end, UInt8 *& dst)
{
	if (src[0] <= ascii_upper_bound)
	{
		if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
			*dst++ = *src++ ^ flip_case_mask;
		else
			*dst++ = *src++;
	}
	else if (src + 1 < src_end
		&& ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) || (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
	{
		cyrillic_to_case(src, src_end, dst);
	}
	else if (src + 1 < src_end && src[0] == 0xC2u)
	{
		/// Пунктуация U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
		*dst++ = *src++;
		*dst++ = *src++;
	}
	else if (src + 2 < src_end && src[0] == 0xE2u)
	{
		/// Символы U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
		*dst++ = *src++;
		*dst++ = *src++;
		*dst++ = *src++;
	}
	else
	{
		static const Poco::UTF8Encoding utf8;

		if (const auto chars = utf8.convert(to_case(utf8.convert(src)), dst, src_end - src))
			src += chars, dst += chars;
		else
			++src, ++dst;
	}
}

template <char not_case_lower_bound,
	char not_case_upper_bound,
	int to_case(int),
	void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::array(
	const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
#if __SSE2__
	const auto bytes_sse = sizeof(__m128i);
	auto src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

	/// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
	const auto v_zero = _mm_setzero_si128();
	const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
	const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
	const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

	while (src < src_end_sse)
	{
		const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

		/// check for ASCII
		const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
		const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

		/// ASCII
		if (mask_is_not_ascii == 0)
		{
			const auto is_not_case
				= _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
			const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

			/// everything in correct case ASCII
			if (mask_is_not_case == 0)
				_mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
			else
			{
				/// ASCII in mixed case
				/// keep `flip_case_mask` only where necessary, zero out elsewhere
				const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

				/// flip case by applying calculated mask
				const auto cased_chars = _mm_xor_si128(chars, xor_mask);

				/// store result back to destination
				_mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
			}

			src += bytes_sse, dst += bytes_sse;
		}
		else
		{
			/// UTF-8
			const auto expected_end = src + bytes_sse;

			while (src < expected_end)
				toCase(src, src_end, dst);

			/// adjust src_end_sse by pushing it forward or backward
			const auto diff = src - expected_end;
			if (diff != 0)
			{
				if (src_end_sse + diff < src_end)
					src_end_sse += diff;
				else
					src_end_sse -= bytes_sse - diff;
			}
		}
	}
#endif
	/// handle remaining symbols
	while (src < src_end)
		toCase(src, src_end, dst);
}


void ReverseImpl::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.resize(data.size());
	res_offsets.assign(offsets);
	size_t size = offsets.size();

	ColumnString::Offset_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
			res_data[j] = data[offsets[i] + prev_offset - 2 - j];
		res_data[offsets[i] - 1] = 0;
		prev_offset = offsets[i];
	}
}

void ReverseImpl::vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
{
	res_data.resize(data.size());
	size_t size = data.size() / n;

	for (size_t i = 0; i < size; ++i)
		for (size_t j = i * n; j < (i + 1) * n; ++j)
			res_data[j] = data[(i * 2 + 1) * n - j - 1];
}

void ReverseImpl::constant(const std::__cxx11::string & data, std::__cxx11::string & res_data)
{
	res_data.resize(data.size());
	for (size_t j = 0; j < data.size(); ++j)
		res_data[j] = data[data.size() - j - 1];
}


void ReverseUTF8Impl::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.resize(data.size());
	res_offsets.assign(offsets);
	size_t size = offsets.size();

	ColumnString::Offset_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		ColumnString::Offset_t j = prev_offset;
		while (j < offsets[i] - 1)
		{
			if (data[j] < 0xBF)
			{
				res_data[offsets[i] + prev_offset - 2 - j] = data[j];
				j += 1;
			}
			else if (data[j] < 0xE0)
			{
				memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 1], &data[j], 2);
				j += 2;
			}
			else if (data[j] < 0xF0)
			{
				memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 2], &data[j], 3);
				j += 3;
			}
			else
			{
				res_data[offsets[i] + prev_offset - 2 - j] = data[j];
				j += 1;
			}
		}

		res_data[offsets[i] - 1] = 0;
		prev_offset = offsets[i];
	}
}

void ReverseUTF8Impl::vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
{
	throw Exception("Cannot apply function reverseUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
}

void ReverseUTF8Impl::constant(const std::__cxx11::string & data, std::__cxx11::string & res_data)
{
	res_data.resize(data.size());

	size_t j = 0;
	while (j < data.size())
	{
		if (static_cast<unsigned char>(data[j]) < 0xBF)
		{
			res_data[data.size() - 1 - j] = data[j];
			j += 1;
		}
		else if (static_cast<unsigned char>(data[j]) < 0xE0)
		{
			memcpy(&res_data[data.size() - 1 - j - 1], &data[j], 2);
			j += 2;
		}
		else if (static_cast<unsigned char>(data[j]) < 0xF0)
		{
			memcpy(&res_data[data.size() - 1 - j - 2], &data[j], 3);
			j += 3;
		}
		else
		{
			res_data[data.size() - 1 - j] = data[j];
			j += 1;
		}
	}
}


void SubstringImpl::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	size_t start,
	size_t length,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.reserve(data.size());
	size_t size = offsets.size();
	res_offsets.resize(size);

	ColumnString::Offset_t prev_offset = 0;
	ColumnString::Offset_t res_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		size_t string_size = offsets[i] - prev_offset;
		if (start >= string_size + 1)
		{
			res_data.resize(res_data.size() + 1);
			res_data[res_offset] = 0;
			++res_offset;
		}
		else
		{
			size_t bytes_to_copy = std::min(offsets[i] - prev_offset - start, length);
			res_data.resize(res_data.size() + bytes_to_copy + 1);
			memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + start - 1], bytes_to_copy);
			res_offset += bytes_to_copy + 1;
			res_data[res_offset - 1] = 0;
		}
		res_offsets[i] = res_offset;
		prev_offset = offsets[i];
	}
}

void SubstringImpl::vector_fixed(const ColumnString::Chars_t & data,
	size_t n,
	size_t start,
	size_t length,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	if (length == 0 || start + length > n + 1)
		throw Exception("Index out of bound for function substring of fixed size value", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

	size_t size = data.size() / n;
	res_offsets.resize(size);
	res_data.resize(length * size + size);

	ColumnString::Offset_t res_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[i * n + start - 1], length);
		res_offset += length;
		res_data[res_offset] = 0;
		++res_offset;
		res_offsets[i] = res_offset;
	}
}

void SubstringImpl::constant(const std::__cxx11::string & data, size_t start, size_t length, std::__cxx11::string & res_data)
{
	if (start + length > data.size() + 1)
		throw Exception("Index out of bound for function substring of fixed size value", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

	res_data = data.substr(start - 1, length);
}


void SubstringUTF8Impl::vector(const ColumnString::Chars_t & data,
	const IColumn::Offsets_t & offsets,
	size_t start,
	size_t length,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	res_data.reserve(data.size());
	size_t size = offsets.size();
	res_offsets.resize(size);

	ColumnString::Offset_t prev_offset = 0;
	ColumnString::Offset_t res_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		ColumnString::Offset_t j = prev_offset;
		ColumnString::Offset_t pos = 1;
		ColumnString::Offset_t bytes_start = 0;
		ColumnString::Offset_t bytes_length = 0;
		while (j < offsets[i] - 1)
		{
			if (pos == start)
				bytes_start = j - prev_offset + 1;

			if (data[j] < 0xBF)
				j += 1;
			else if (data[j] < 0xE0)
				j += 2;
			else if (data[j] < 0xF0)
				j += 3;
			else
				j += 1;

			if (pos >= start && pos < start + length)
				bytes_length = j - prev_offset + 1 - bytes_start;
			else if (pos >= start + length)
				break;

			++pos;
		}

		if (bytes_start == 0)
		{
			res_data.resize(res_data.size() + 1);
			res_data[res_offset] = 0;
			++res_offset;
		}
		else
		{
			size_t bytes_to_copy = std::min(offsets[i] - prev_offset - bytes_start, bytes_length);
			res_data.resize(res_data.size() + bytes_to_copy + 1);
			memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + bytes_start - 1], bytes_to_copy);
			res_offset += bytes_to_copy + 1;
			res_data[res_offset - 1] = 0;
		}
		res_offsets[i] = res_offset;
		prev_offset = offsets[i];
	}
}

void SubstringUTF8Impl::vector_fixed(const ColumnString::Chars_t & data,
	IColumn::Offset_t n,
	size_t start,
	size_t length,
	ColumnString::Chars_t & res_data,
	IColumn::Offsets_t & res_offsets)
{
	throw Exception("Cannot apply function substringUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
}

void SubstringUTF8Impl::constant(const std::__cxx11::string & data, size_t start, size_t length, std::__cxx11::string & res_data)
{
	if (start + length > data.size() + 1)
		throw Exception("Index out of bound for function substring of constant value", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

	ColumnString::Offset_t j = 0;
	ColumnString::Offset_t pos = 1;
	ColumnString::Offset_t bytes_start = 0;
	ColumnString::Offset_t bytes_length = 0;
	while (j < data.size())
	{
		if (pos == start)
			bytes_start = j + 1;

		if (static_cast<unsigned char>(data[j]) < 0xBF)
			j += 1;
		else if (static_cast<unsigned char>(data[j]) < 0xE0)
			j += 2;
		else if (static_cast<unsigned char>(data[j]) < 0xF0)
			j += 3;
		else
			j += 1;

		if (pos >= start && pos < start + length)
			bytes_length = j + 1 - bytes_start;
		else if (pos >= start + length)
			break;

		++pos;
	}

	if (bytes_start != 0)
		res_data = data.substr(bytes_start - 1, bytes_length);
}


template <typename Impl, typename Name, typename ResultType>
FunctionPtr FunctionStringOrArrayToT<Impl, Name, ResultType>::create(const Context & context)
{
	return std::make_shared<FunctionStringOrArrayToT>();
}

template <typename Impl, typename Name, typename ResultType>
DataTypePtr FunctionStringOrArrayToT<Impl, Name, ResultType>::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0])
		&& !typeid_cast<const DataTypeArray *>(&*arguments[0]))
		throw Exception(
			"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return std::make_shared<typename DataTypeFromFieldType<ResultType>::Type>();
}

template <typename Impl, typename Name, typename ResultType>
void FunctionStringOrArrayToT<Impl, Name, ResultType>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
	if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
	{
		auto col_res = std::make_shared<ColumnVector<ResultType>>();
		block.safeGetByPosition(result).column = col_res;

		typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
		vec_res.resize(col->size());
		Impl::vector(col->getChars(), col->getOffsets(), vec_res);
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(&*column))
	{
		/// Для фиксированной строки, только функция lengthUTF8 возвращает не константу.
		if ("lengthUTF8" != getName())
		{
			ResultType res = 0;
			Impl::vector_fixed_to_constant(col->getChars(), col->getN(), res);

			auto col_res = std::make_shared<ColumnConst<ResultType>>(col->size(), res);
			block.safeGetByPosition(result).column = col_res;
		}
		else
		{
			auto col_res = std::make_shared<ColumnVector<ResultType>>();
			block.safeGetByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			Impl::vector_fixed_to_vector(col->getChars(), col->getN(), vec_res);
		}
	}
	else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
	{
		ResultType res = 0;
		Impl::constant(col->getData(), res);

		auto col_res = std::make_shared<ColumnConst<ResultType>>(col->size(), res);
		block.safeGetByPosition(result).column = col_res;
	}
	else if (const ColumnArray * col = typeid_cast<const ColumnArray *>(&*column))
	{
		auto col_res = std::make_shared<ColumnVector<ResultType>>();
		block.safeGetByPosition(result).column = col_res;

		typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
		vec_res.resize(col->size());
		Impl::array(col->getOffsets(), vec_res);
	}
	else if (const ColumnConstArray * col = typeid_cast<const ColumnConstArray *>(&*column))
	{
		ResultType res = 0;
		Impl::constant_array(col->getData(), res);

		auto col_res = std::make_shared<ColumnConst<ResultType>>(col->size(), res);
		block.safeGetByPosition(result).column = col_res;
	}
	else
		throw Exception(
			"Illegal column " + block.safeGetByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
}

  template <typename Name, bool is_injective>
  FunctionPtr ConcatImpl<Name, is_injective>::create(const Context& context)
  	{
  		return std::make_shared<ConcatImpl>();
  	}
  template <typename Name, bool is_injective>
  DataTypePtr ConcatImpl<Name, is_injective>::getReturnTypeImpl(const DataTypes& arguments) const
  	{
  		if (arguments.size() < 2)
  			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
  					+ ", should be at least 2.",
  				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

  		for (const auto arg_idx : ext::range(0, arguments.size()))
  		{
  			const auto arg = arguments[arg_idx].get();
  			if (!typeid_cast<const DataTypeString *>(arg) && !typeid_cast<const DataTypeFixedString *>(arg))
  				throw Exception{
  					"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
  					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
  		}

  		return std::make_shared<DataTypeString>();
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::executeImpl(Block& block, const ColumnNumbers& arguments, const size_t result)
  	{
  		if (arguments.size() == 2)
  			executeBinary(block, arguments, result);
  		else
  			executeNAry(block, arguments, result);
  	}
  template <typename Name, bool is_injective>
  typename ConcatImpl<Name, is_injective>::Instructions ConcatImpl<Name, is_injective>::getInstructions(const Block& block, const ColumnNumbers& arguments, size_t& out_length, bool& out_const)
  	{
  		Instructions result{};
  		result.reserve(arguments.size());

  		out_length = 0;
  		out_const = true;

  		size_t rows{};
  		for (const auto arg_pos : arguments)
  		{
  			const auto column = block.safeGetByPosition(arg_pos).column.get();

  			if (const auto col = typeid_cast<const ColumnString *>(column))
  			{
  				/** ColumnString stores strings with terminating null character
  				  *  which should not be copied, therefore the decrease of total size by
  				  *	the number of terminating nulls
  				  */
  				rows = col->size();
  				out_length += col->getChars().size() - col->getOffsets().size();
  				out_const = false;

  				result.emplace_back(InstructionType::COPY_STRING, ColumnAndOffset{col, 0});
  			}
  			else if (const auto col = typeid_cast<const ColumnFixedString *>(column))
  			{
  				rows = col->size();
  				out_length += col->getChars().size();
  				out_const = false;

  				result.emplace_back(InstructionType::COPY_FIXED_STRING, ColumnAndOffset{col, 0});
  			}
  			else if (const auto col = typeid_cast<const ColumnConstString *>(column))
  			{
  				rows = col->size();
  				out_length += col->getData().size() * col->size();
  				out_const = out_const && true;

  				result.emplace_back(InstructionType::COPY_CONST_STRING, ColumnAndOffset{col, 0});
  			}
  			else
  				throw Exception(
  					"Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
  		}

  		if (out_const && rows)
  			out_length /= rows;

  		return result;
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::executeBinary(Block& block, const ColumnNumbers& arguments, const size_t result)
  	{
  		const IColumn * c0 = block.safeGetByPosition(arguments[0]).column.get();
  		const IColumn * c1 = block.safeGetByPosition(arguments[1]).column.get();

  		const ColumnString * c0_string = typeid_cast<const ColumnString *>(c0);
  		const ColumnString * c1_string = typeid_cast<const ColumnString *>(c1);
  		const ColumnFixedString * c0_fixed_string = typeid_cast<const ColumnFixedString *>(c0);
  		const ColumnFixedString * c1_fixed_string = typeid_cast<const ColumnFixedString *>(c1);
  		const ColumnConstString * c0_const = typeid_cast<const ColumnConstString *>(c0);
  		const ColumnConstString * c1_const = typeid_cast<const ColumnConstString *>(c1);

  		/// Результат - const string
  		if (c0_const && c1_const)
  		{
  			auto c_res = std::make_shared<ColumnConstString>(c0_const->size(), "");
  			block.safeGetByPosition(result).column = c_res;
  			constant_constant(c0_const->getData(), c1_const->getData(), c_res->getData());
  		}
  		else
  		{
  			auto c_res = std::make_shared<ColumnString>();
  			block.safeGetByPosition(result).column = c_res;
  			ColumnString::Chars_t & vec_res = c_res->getChars();
  			ColumnString::Offsets_t & offsets_res = c_res->getOffsets();

  			if (c0_string && c1_string)
  				vector_vector(
  					c0_string->getChars(), c0_string->getOffsets(), c1_string->getChars(), c1_string->getOffsets(), vec_res, offsets_res);
  			else if (c0_string && c1_fixed_string)
  				vector_fixed_vector(c0_string->getChars(),
  					c0_string->getOffsets(),
  					c1_fixed_string->getChars(),
  					c1_fixed_string->getN(),
  					vec_res,
  					offsets_res);
  			else if (c0_string && c1_const)
  				vector_constant(c0_string->getChars(), c0_string->getOffsets(), c1_const->getData(), vec_res, offsets_res);
  			else if (c0_fixed_string && c1_string)
  				fixed_vector_vector(c0_fixed_string->getChars(),
  					c0_fixed_string->getN(),
  					c1_string->getChars(),
  					c1_string->getOffsets(),
  					vec_res,
  					offsets_res);
  			else if (c0_const && c1_string)
  				constant_vector(c0_const->getData(), c1_string->getChars(), c1_string->getOffsets(), vec_res, offsets_res);
  			else if (c0_fixed_string && c1_fixed_string)
  				fixed_vector_fixed_vector(c0_fixed_string->getChars(),
  					c0_fixed_string->getN(),
  					c1_fixed_string->getChars(),
  					c1_fixed_string->getN(),
  					vec_res,
  					offsets_res);
  			else if (c0_fixed_string && c1_const)
  				fixed_vector_constant(c0_fixed_string->getChars(), c0_fixed_string->getN(), c1_const->getData(), vec_res, offsets_res);
  			else if (c0_const && c1_fixed_string)
  				constant_fixed_vector(c0_const->getData(), c1_fixed_string->getChars(), c1_fixed_string->getN(), vec_res, offsets_res);
  			else
  				throw Exception("Illegal columns " + block.safeGetByPosition(arguments[0]).column->getName() + " and "
  						+ block.safeGetByPosition(arguments[1]).column->getName()
  						+ " of arguments of function "
  						+ getName(),
  					ErrorCodes::ILLEGAL_COLUMN);
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::executeNAry(Block& block, const ColumnNumbers& arguments, const size_t result)
  	{
  		const auto size = block.rows();
  		std::size_t result_length{};
  		bool result_is_const{};
  		auto instrs = getInstructions(block, arguments, result_length, result_is_const);

  		if (result_is_const)
  		{
  			const auto out = std::make_shared<ColumnConstString>(size, "");
  			block.safeGetByPosition(result).column = out;

  			auto & data = out->getData();
  			data.reserve(result_length);

  			for (const auto & instr : instrs)
  				data += static_cast<const ColumnConst<String> *>(instr.second.first)->getData();
  		}
  		else
  		{
  			const auto out = std::make_shared<ColumnString>();
  			block.safeGetByPosition(result).column = out;

  			auto & out_data = out->getChars();
  			out_data.resize(result_length + size);

  			auto & out_offsets = out->getOffsets();
  			out_offsets.resize(size);

  			std::size_t out_offset{};

  			for (const auto row : ext::range(0, size))
  			{
  				for (auto & instr : instrs)
  				{
  					switch (instr.first)
  					{
  						case InstructionType::COPY_STRING:
  						{
  							auto & in_offset = instr.second.second;
  							const auto col = static_cast<const ColumnString *>(instr.second.first);
  							const auto offset = col->getOffsets()[row];
  							const auto length = offset - in_offset - 1;

  							memcpySmallAllowReadWriteOverflow15(&out_data[out_offset], &col->getChars()[in_offset], length);
  							out_offset += length;
  							in_offset = offset;
  							break;
  						}
  						case InstructionType::COPY_FIXED_STRING:
  						{
  							auto & in_offset = instr.second.second;
  							const auto col = static_cast<const ColumnFixedString *>(instr.second.first);
  							const auto length = col->getN();

  							memcpySmallAllowReadWriteOverflow15(&out_data[out_offset], &col->getChars()[in_offset], length);
  							out_offset += length;
  							in_offset += length;
  							break;
  						}
  						case InstructionType::COPY_CONST_STRING:
  						{
  							const auto col = static_cast<const ColumnConst<String> *>(instr.second.first);
  							const auto & data = col->getData();
  							const auto length = data.size();

  							memcpy(&out_data[out_offset], data.data(), length);
  							out_offset += length;
  							break;
  						}
  						default:
  							throw Exception("Unknown InstructionType during execution of function 'concat'", ErrorCodes::LOGICAL_ERROR);
  					}
  				}

  				out_data[out_offset] = '\0';
  				out_offsets[row] = ++out_offset;
  			}
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::vector_vector(const ColumnString::Chars_t& a_data, const IColumn::Offsets_t& a_offsets, const ColumnString::Chars_t& b_data, const IColumn::Offsets_t& b_offsets, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = a_offsets.size();
  		c_data.resize(a_data.size() + b_data.size() - size);
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		ColumnString::Offset_t a_offset = 0;
  		ColumnString::Offset_t b_offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[a_offset], a_offsets[i] - a_offset - 1);
  			offset += a_offsets[i] - a_offset - 1;
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[b_offset], b_offsets[i] - b_offset);
  			offset += b_offsets[i] - b_offset;

  			a_offset = a_offsets[i];
  			b_offset = b_offsets[i];

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::vector_fixed_vector(const ColumnString::Chars_t& a_data, const IColumn::Offsets_t& a_offsets, const ColumnString::Chars_t& b_data, IColumn::Offset_t b_n, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = a_offsets.size();
  		c_data.resize(a_data.size() + b_data.size());
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		ColumnString::Offset_t a_offset = 0;
  		ColumnString::Offset_t b_offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[a_offset], a_offsets[i] - a_offset - 1);
  			offset += a_offsets[i] - a_offset - 1;
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[b_offset], b_n);
  			offset += b_n;
  			c_data[offset] = 0;
  			offset += 1;

  			a_offset = a_offsets[i];
  			b_offset += b_n;

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::vector_constant(const ColumnString::Chars_t& a_data, const IColumn::Offsets_t& a_offsets, const std::__cxx11::string& b, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = a_offsets.size();
  		c_data.resize(a_data.size() + b.size() * size);
  		c_offsets.assign(a_offsets);

  		for (size_t i = 0; i < size; ++i)
  			c_offsets[i] += b.size() * (i + 1);

  		ColumnString::Offset_t offset = 0;
  		ColumnString::Offset_t a_offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[a_offset], a_offsets[i] - a_offset - 1);
  			offset += a_offsets[i] - a_offset - 1;
  			memcpy(&c_data[offset], b.data(), b.size() + 1);
  			offset += b.size() + 1;

  			a_offset = a_offsets[i];
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::fixed_vector_vector(const ColumnString::Chars_t& a_data, IColumn::Offset_t a_n, const ColumnString::Chars_t& b_data, const IColumn::Offsets_t& b_offsets, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = b_offsets.size();
  		c_data.resize(a_data.size() + b_data.size());
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		ColumnString::Offset_t a_offset = 0;
  		ColumnString::Offset_t b_offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[a_offset], a_n);
  			offset += a_n;
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[b_offset], b_offsets[i] - b_offset);
  			offset += b_offsets[i] - b_offset;

  			a_offset = a_n;
  			b_offset = b_offsets[i];

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::fixed_vector_fixed_vector(const ColumnString::Chars_t& a_data, IColumn::Offset_t a_n, const ColumnString::Chars_t& b_data, IColumn::Offset_t b_n, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = a_data.size() / a_n;
  		c_data.resize(a_data.size() + b_data.size() + size);
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[i * a_n], a_n);
  			offset += a_n;
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[i * b_n], b_n);
  			offset += b_n;
  			c_data[offset] = 0;
  			++offset;

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::fixed_vector_constant(const ColumnString::Chars_t& a_data, IColumn::Offset_t a_n, const std::__cxx11::string& b, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = a_data.size() / a_n;
  		ColumnString::Offset_t b_n = b.size();
  		c_data.resize(a_data.size() + size * b_n + size);
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &a_data[i * a_n], a_n);
  			offset += a_n;
  			memcpy(&c_data[offset], b.data(), b_n);
  			offset += b_n;
  			c_data[offset] = 0;
  			++offset;

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::constant_vector(const std::__cxx11::string& a, const ColumnString::Chars_t& b_data, const IColumn::Offsets_t& b_offsets, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = b_offsets.size();
  		c_data.resize(b_data.size() + a.size() * size);
  		c_offsets.assign(b_offsets);

  		for (size_t i = 0; i < size; ++i)
  			c_offsets[i] += a.size() * (i + 1);

  		ColumnString::Offset_t offset = 0;
  		ColumnString::Offset_t b_offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpy(&c_data[offset], a.data(), a.size());
  			offset += a.size();
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[b_offset], b_offsets[i] - b_offset);
  			offset += b_offsets[i] - b_offset;

  			b_offset = b_offsets[i];
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::constant_fixed_vector(const std::__cxx11::string& a, const ColumnString::Chars_t& b_data, IColumn::Offset_t b_n, ColumnString::Chars_t& c_data, IColumn::Offsets_t& c_offsets)
  	{
  		size_t size = b_data.size() / b_n;
  		ColumnString::Offset_t a_n = a.size();
  		c_data.resize(size * a_n + b_data.size() + size);
  		c_offsets.resize(size);

  		ColumnString::Offset_t offset = 0;
  		for (size_t i = 0; i < size; ++i)
  		{
  			memcpy(&c_data[offset], a.data(), a_n);
  			offset += a_n;
  			memcpySmallAllowReadWriteOverflow15(&c_data[offset], &b_data[i * b_n], b_n);
  			offset += b_n;
  			c_data[offset] = 0;
  			++offset;

  			c_offsets[i] = offset;
  		}
  	}
  template <typename Name, bool is_injective>
  void ConcatImpl<Name, is_injective>::constant_constant(const std::__cxx11::string& a, const std::__cxx11::string& b, std::__cxx11::string& c)
  	{
  		c = a + b;
  	}

}
