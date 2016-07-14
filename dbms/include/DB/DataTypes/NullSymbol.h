#pragma once

#include <DB/Columns/IColumn.h>

namespace DB
{

namespace
{

constexpr size_t strlen_constexpr(const char * in)
{
	return (*in == '\0') ? 0 : 1 + strlen_constexpr(in + 1);
}

}

namespace NullSymbol
{

struct Plain
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen_constexpr(name);
};

struct Escaped
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen_constexpr(name);
};

struct Quoted
{
	static constexpr auto prefix = 'N';
	static constexpr auto suffix = "ULL";
	static constexpr auto name = "NULL";
	static constexpr auto length = strlen_constexpr(name);
};

struct CSV
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen_constexpr(name);
};

struct JSON
{
	static constexpr auto prefix = 'n';
	static constexpr auto suffix = "ull";
	static constexpr auto name = "null";
	static constexpr auto length = strlen_constexpr(name);
};

struct XML
{
	static constexpr auto prefix = '\\';
	static constexpr auto suffix = "N";
	static constexpr auto name = "\\N";
	static constexpr auto length = strlen_constexpr(name);
};

}

}
