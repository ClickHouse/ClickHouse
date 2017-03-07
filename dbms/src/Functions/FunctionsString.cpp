#include <DB/Functions/FunctionsString.h>

#include <DB/Functions/FunctionsArray.h>
#include <DB/Functions/FunctionFactory.h>


namespace DB
{

String FunctionReverse::getName() const
{
	return name;
}


DataTypePtr FunctionReverse::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0])
		&& !typeid_cast<const DataTypeArray *>(&*arguments[0]))
		throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return arguments[0]->clone();
}


void FunctionReverse::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
	if (const ColumnString * col = typeid_cast<const ColumnString *>(column.get()))
	{
		std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
		block.safeGetByPosition(result).column = col_res;
		ReverseImpl::vector(col->getChars(), col->getOffsets(),
						col_res->getChars(), col_res->getOffsets());
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(column.get()))
	{
		auto col_res = std::make_shared<ColumnFixedString>(col->getN());
		block.safeGetByPosition(result).column = col_res;
		ReverseImpl::vector_fixed(col->getChars(), col->getN(),
							col_res->getChars());
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
		throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
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
			"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};

	if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
		throw Exception{
			"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};

	return std::make_shared<DataTypeString>();
}


void FunctionAppendTrailingCharIfAbsent::executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
{
	const auto & column = block.safeGetByPosition(arguments[0]).column;
	const auto & column_char = block.safeGetByPosition(arguments[1]).column;

	if (!typeid_cast<const ColumnConstString *>(column_char.get()))
		throw Exception{
			"Second argument of function " + getName() + " must be a constant string",
			ErrorCodes::ILLEGAL_COLUMN
		};

	const auto & trailing_char_str = static_cast<const ColumnConstString &>(*column_char).getData();

	if (trailing_char_str.size() != 1)
		throw Exception{
			"Second argument of function " + getName() + " must be a one-character string",
			ErrorCodes::BAD_ARGUMENTS
		};

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

		block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(
			col->size(),
			in_data.size() == 0 ? in_data :
				in_data.back() == trailing_char_str.front() ? in_data : in_data + trailing_char_str);
	}
	else
		throw Exception{
			"Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN
		};
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
void EmptyImpl<negative>::vector(const ColumnString::Chars_t& data, const IColumn::Offsets_t& offsets, PaddedPODArray< UInt8 >& res)
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
	void EmptyImpl<negative>::vector_fixed_to_constant(const ColumnString::Chars_t& data, size_t n, UInt8& res)
	{
		res = negative ^ (n == 0);
	}
template <bool negative>
	void EmptyImpl<negative>::constant(const std::__cxx11::string& data, UInt8& res)
	{
		res = negative ^ (data.empty());
	}
template <bool negative>
	void EmptyImpl<negative>::array(const IColumn::Offsets_t& offsets, PaddedPODArray< UInt8 >& res)
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
	void EmptyImpl<negative>::constant_array(const Array& data, UInt8& res)
	{
		res = negative ^ (data.empty());
	}
template <bool negative>
	void EmptyImpl<negative>::vector_fixed_to_vector(const ColumnString::Chars_t& data, size_t n, PaddedPODArray< UInt8 >& res)
	{
	}
void LengthImpl::vector(const ColumnString::Chars_t& data, const IColumn::Offsets_t& offsets, PaddedPODArray< UInt64 >& res)
	{
		size_t size = offsets.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = i == 0
				? (offsets[i] - 1)
				: (offsets[i] - 1 - offsets[i - 1]);
	}
void LengthImpl::vector_fixed_to_vector(const ColumnString::Chars_t& data, size_t n, PaddedPODArray< UInt64 >& res)
	{
	}
void LengthImpl::constant(const std::__cxx11::string& data, UInt64& res)
	{
		res = data.size();
	}
void LengthImpl::array(const IColumn::Offsets_t& offsets, PaddedPODArray< UInt64 >& res)
	{
		size_t size = offsets.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = i == 0
				? (offsets[i])
				: (offsets[i] - offsets[i - 1]);
	}
void LengthImpl::constant_array(const Array& data, UInt64& res)
	{
		res = data.size();
	}
void LengthUTF8Impl::vector(const ColumnString::Chars_t& data, const IColumn::Offsets_t& offsets, PaddedPODArray< UInt64 >& res)
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
void LengthUTF8Impl::vector_fixed_to_constant(const ColumnString::Chars_t& data, size_t n, UInt64& res)
	{
	}
void LengthUTF8Impl::vector_fixed_to_vector(const ColumnString::Chars_t& data, size_t n, PaddedPODArray< UInt64 >& res)
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
void LengthUTF8Impl::constant(const std::__cxx11::string& data, UInt64& res)
	{
		res = 0;
		for (const UInt8 * c = reinterpret_cast<const UInt8 *>(data.data()); c < reinterpret_cast<const UInt8 *>(data.data() + data.size()); ++c)
			if (*c <= 0x7F || *c >= 0xC0)
				++res;
	}

	void LengthUTF8Impl::array(const IColumn::Offsets_t& offsets, PaddedPODArray< UInt64 >& res)
	{
		throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void LengthUTF8Impl::constant_array(const Array& data, UInt64& res)
	{
		throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}


	template <char not_case_lower_bound, char not_case_upper_bound>
	void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector(const ColumnString::Chars_t& data, const IColumn::Offsets_t& offsets, ColumnString::Chars_t& res_data, IColumn::Offsets_t& res_offsets)
	{
		res_data.resize(data.size());
		res_offsets.assign(offsets);
		array(data.data(), data.data() + data.size(), res_data.data());
	}

	template <char not_case_lower_bound, char not_case_upper_bound>
	void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::constant(const std::__cxx11::string& data, std::__cxx11::string& res_data)
	{
		res_data.resize(data.size());
		array(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data() + data.size()),
			  reinterpret_cast<UInt8 *>(&res_data[0]));
	}

	template <char not_case_lower_bound, char not_case_upper_bound>
	void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::array(const UInt8* src, const UInt8* src_end, UInt8* dst)
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
			const auto is_not_case = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound),
												   _mm_cmplt_epi8(chars, v_not_case_upper_bound));

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
	void LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector_fixed(const ColumnString::Chars_t& data, size_t n, ColumnString::Chars_t& res_data)
	{
		res_data.resize(data.size());
		array(data.data(), data.data() + data.size(), res_data.data());
	}

	template <bool to_lower>
	void UTF8CyrillicToCase(const UInt8*& src, const UInt8*const src_end, UInt8*& dst)
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
template <char not_case_lower_bound, char not_case_upper_bound,
	int to_case(int), void cyrillic_to_case(const UInt8 * &, const UInt8 *, UInt8 * &)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector(const ColumnString::Chars_t& data, const IColumn::Offsets_t& offsets, ColumnString::Chars_t& res_data, IColumn::Offsets_t& res_offsets)
	{
		res_data.resize(data.size());
		res_offsets.assign(offsets);
		array(data.data(), data.data() + data.size(), res_data.data());
	}
template <char not_case_lower_bound, char not_case_upper_bound,
	int to_case(int), void cyrillic_to_case(const UInt8 * &, const UInt8 *, UInt8 * &)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector_fixed(const ColumnString::Chars_t& data, size_t n, ColumnString::Chars_t& res_data)
	{
		res_data.resize(data.size());
		array(data.data(), data.data() + data.size(), res_data.data());
	}
template <char not_case_lower_bound, char not_case_upper_bound,
	int to_case(int), void cyrillic_to_case(const UInt8 * &, const UInt8 *, UInt8 * &)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::constant(const std::__cxx11::string& data, std::__cxx11::string& res_data)
	{
		res_data.resize(data.size());
		array(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data() + data.size()),
			  reinterpret_cast<UInt8 *>(&res_data[0]));
	}
template <char not_case_lower_bound, char not_case_upper_bound,
	int to_case(int), void cyrillic_to_case(const UInt8 * &, const UInt8 *, UInt8 * &)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(const UInt8*& src, const UInt8*const src_end, UInt8*& dst)
	{
		if (src[0] <= ascii_upper_bound)
		{
			if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
				*dst++ = *src++ ^ flip_case_mask;
			else
				*dst++ = *src++;
		}
		else if (src + 1 < src_end &&
				 ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) ||
				  (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
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

}
