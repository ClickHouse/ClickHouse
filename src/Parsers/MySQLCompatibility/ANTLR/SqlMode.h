#pragma once

class SqlMode
{
public:
    enum
    {
        NoMode = 0,
        AnsiQuotes = 1 << 0,
        HighNotPrecedence = 1 << 1, // MOO: others
        NoBackslashEscapes = 1 << 4
    };
};
