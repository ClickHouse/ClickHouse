#pragma once

#include <cstring>

namespace DB
{

namespace NullSymbol
{

struct Plain
{
	static constexpr auto name = "\\N";
};

struct Escaped
{
	static constexpr auto name = "\\N";
};

struct Quoted
{
	static constexpr auto name = "NULL";
};

struct CSV
{
	static constexpr auto name = "\\N";
};

struct JSON
{
	static constexpr auto name = "null";
};

struct XML
{
	static constexpr auto name = "\\N";
};

}

}
