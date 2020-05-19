#pragma once

#include "LineReader.h"

#include <readline/readline.h>
#include <readline/history.h>

class ReadlineLineReader : public LineReader
{
public:
    ReadlineLineReader(const Suggest & suggest, const String & history_file_path, char extender, char delimiter = 0);
    ~ReadlineLineReader() override;

    void enableBracketedPaste() override;

private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;
};
