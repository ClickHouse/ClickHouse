#pragma once

#include <unordered_map>

#include "LineReader.h"
#include <Parsers/Lexer.h>

#include <replxx.hxx>


class ReplxxLineReader : public LineReader
{
public:
    ReplxxLineReader(const Suggest & suggest, const String & history_file_path, bool multiline, Patterns extenders_, Patterns delimiters_);
    ~ReplxxLineReader() override;

    void enableBracketedPaste() override;

private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;

    replxx::Replxx rx;
};
