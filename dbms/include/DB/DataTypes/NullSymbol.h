#pragma once

#include <cstring>

namespace DB
{

namespace NullSymbol
{

struct Plain
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen(name);
};

struct Escaped
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen(name);
};

struct Quoted
{
	static constexpr auto prefix = 'N';
	static constexpr auto suffix = "ULL";
	static constexpr auto name = "NULL";
	static constexpr auto length = strlen(name);
};

struct CSV
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen(name);
};

struct JSON
{
	static constexpr auto prefix = 'n';
	static constexpr auto suffix = "ull";
	static constexpr auto name = "null";
	static constexpr auto length = strlen(name);
};

struct XML
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen(name);
};

}

}
