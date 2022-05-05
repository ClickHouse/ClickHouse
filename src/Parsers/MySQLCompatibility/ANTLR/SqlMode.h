#pragma once

enum SqlMode
{
	NoMode = 0u,
	AnsiQuotes = 1u << 0,
	HighNotPrecedence = 1u << 1,
	PipesAsConcat = 1u << 2,
	IgnoreSpace = 1u << 3,
	NoBackslashEscapes = 1u << 4
};
