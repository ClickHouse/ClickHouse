#include <DB/Common/hex.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsURL.h>
#include <common/find_first_symbols.h>

namespace DB
{

/// We assume that size of the dst buf isn't less than src_size.
static size_t decodeURL(const char * src, size_t src_size, char * dst)
{
	const char * src_prev_pos = src;
	const char * src_curr_pos = src;
	const char * const src_end = src + src_size;
	char * dst_pos = dst;

	while (true)
	{
		src_curr_pos = find_first_symbols<'%'>(src_curr_pos, src_end);

		if (src_curr_pos == src_end)
			break;
		else if (src_end - src_curr_pos < 3)
		{
			src_curr_pos = src_end;
			break;
		}
		else
		{
			unsigned char high = char_to_digit_table[static_cast<unsigned char>(src_curr_pos[1])];
			unsigned char low = char_to_digit_table[static_cast<unsigned char>(src_curr_pos[2])];

			if (high != 0xFF && low != 0xFF)
			{
				unsigned char octet = (high << 4) + low;

				size_t bytes_to_copy = src_curr_pos - src_prev_pos;
				memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
				dst_pos += bytes_to_copy;

				*dst_pos = octet;
				++dst_pos;

				src_prev_pos = src_curr_pos + 3;
			}

			src_curr_pos += 3;
		}
	}

	if (src_prev_pos < src_curr_pos)
	{
		size_t bytes_to_copy = src_curr_pos - src_prev_pos;
		memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
		dst_pos += bytes_to_copy;
	}

	return dst_pos - dst;
}


size_t ExtractProtocol::getReserveLengthForElement()
{
	return makeStringView("https").size() + 1;
}


void ExtractProtocol::execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
{
	res_data = data;
	res_size = 0;

	StringView scheme = getURLScheme(StringView(data, size));
	Pos pos = data + scheme.size();

	if (scheme.empty() || (data + size) - pos < 4)
		return;

	if (pos[0] == ':')
		res_size = pos - data;
}


void DecodeURLComponentImpl::vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
	ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
{
	res_data.resize(data.size());
	size_t size = offsets.size();
	res_offsets.resize(size);

	size_t prev_offset = 0;
	size_t res_offset = 0;

	for (size_t i = 0; i < size; ++i)
	{
		const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
		size_t src_size = offsets[i] - prev_offset;
		size_t dst_size = decodeURL(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset));

		res_offset += dst_size;
		res_offsets[i] = res_offset;
		prev_offset = offsets[i];
	}

	res_data.resize(res_offset);
}


void DecodeURLComponentImpl::constant(const std::string & str,
	std::string & res_data)
{
	ColumnString src;
	ColumnString dst;
	src.insert(str);

	vector(src.getChars(), src.getOffsets(), dst.getChars(), dst.getOffsets());

	res_data = dst[0].get<String>();
}


void DecodeURLComponentImpl::vector_fixed(const ColumnString::Chars_t & data, size_t n,
	ColumnString::Chars_t & res_data)
{
	throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
}


void registerFunctionsURL(FunctionFactory & factory)
{
	factory.registerFunction<FunctionProtocol>();
	factory.registerFunction<FunctionDomain>();
	factory.registerFunction<FunctionDomainWithoutWWW>();
	factory.registerFunction<FunctionFirstSignificantSubdomain>();
	factory.registerFunction<FunctionTopLevelDomain>();
	factory.registerFunction<FunctionPath>();
	factory.registerFunction<FunctionPathFull>();
	factory.registerFunction<FunctionQueryString>();
	factory.registerFunction<FunctionFragment>();
	factory.registerFunction<FunctionQueryStringAndFragment>();
	factory.registerFunction<FunctionExtractURLParameter>();
	factory.registerFunction<FunctionExtractURLParameters>();
	factory.registerFunction<FunctionExtractURLParameterNames>();
	factory.registerFunction<FunctionURLHierarchy>();
	factory.registerFunction<FunctionURLPathHierarchy>();
	factory.registerFunction<FunctionCutToFirstSignificantSubdomain>();
	factory.registerFunction<FunctionCutWWW>();
	factory.registerFunction<FunctionCutQueryString>();
	factory.registerFunction<FunctionCutFragment>();
	factory.registerFunction<FunctionCutQueryStringAndFragment>();
	factory.registerFunction<FunctionCutURLParameter>();
	factory.registerFunction<FunctionDecodeURLComponent>();
}

}
