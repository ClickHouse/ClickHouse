#pragma once

#include "LineReader.h"

#include <replxx.hxx>

class ReplxxLineReader : public LineReader
{
public:
    ReplxxLineReader(const Suggest & suggest, const String & history_file_path, char extender, char delimiter = 0);
    ~ReplxxLineReader() override;

    void enableBracketedPaste() override;

private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;

    replxx::Replxx rx;
};
