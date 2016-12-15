#include <DB/Common/hex.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsURL.h>
#include <common/find_first_symbols.h>

namespace DB
{

/// We assume that size of the buf isn't less than url.size().
static size_t decodeURL(const StringView & url, char * dst)
{
	const char * p = url.data();
	const char * st = url.data();
	const char * const end = url.data() + url.size();
	char * buf = dst;

	while (true)
	{
		p = find_first_symbols<'%'>(p, end);

		if (p == end)
			break;
		else if (end - p < 3)
		{
			p = end;
			break;
		}
		else
		{
			unsigned char h = char_to_digit_table[static_cast<unsigned char>(p[1])];
			unsigned char l = char_to_digit_table[static_cast<unsigned char>(p[2])];

			if (h != 0xFF && l != 0xFF)
			{
				unsigned char digit = (h << 4) + l;

				memcpy(buf, st, p - st);
				buf += p - st;
				*buf = digit;
				++buf;
				st = p + 3;
			}

			p = p + 3;
		}
	}

	if (st < p)
	{
		memcpy(buf, st, p - st);
		buf += p - st;
	}

	return buf - dst;
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
	res_data.reserve(data.size());
	size_t size = offsets.size();
	res_offsets.resize(size);

	size_t prev_offset = 0;
	size_t res_offset = 0;

	for (size_t i = 0; i < size; ++i)
	{
		const char * current = reinterpret_cast<const char *>(&data[prev_offset]);
		const StringView url(current, offsets[i] - prev_offset - 1);
		size_t prev_size = res_data.size();

		res_data.resize(prev_size + url.size() + 1);
		size_t len = decodeURL(url, reinterpret_cast<char *>(res_data.data() + res_offset));
		res_data.resize(prev_size + len);
		res_offset += len;
		res_data[res_offset] = 0;
		res_offset++;

		res_offsets[i] = res_offset;
		prev_offset = offsets[i];
	}
}


void DecodeURLComponentImpl::constant(const std::string & data,
	std::string & res_data)
{
	res_data.resize(data.size());
	size_t len = decodeURL(data, &res_data[0]);
	res_data.resize(len);
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
