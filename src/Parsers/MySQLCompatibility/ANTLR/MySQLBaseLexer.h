#pragma once

#include "antlr4-runtime.h"

// using namespace antlr4;
// using namespace antlr4::atn;

class MySQLBaseLexer : public antlr4::Lexer
{
public:
    enum
    {
        CURDATE_SYMBOL,
        ADDDATE_SYMBOL,
        BIT_AND_SYMBOL,
        BIT_OR_SYMBOL,
        BIT_XOR_SYMBOL,
        CAST_SYMBOL,
        COUNT_SYMBOL,
        CURTIME_SYMBOL,
        DATE_ADD_SYMBOL,
        MAX_SYMBOL,
        SUBSTRING_SYMBOL,
        GROUP_CONCAT_SYMBOL,
        MIN_SYMBOL,
        EXTRACT_SYMBOL,
        DATE_SUB_SYMBOL,
        NOT2_SYMBOL,
        NOT_SYMBOL,
        NOW_SYMBOL,
        POSITION_SYMBOL,
        USER_SYMBOL,
        STDDEV_SAMP_SYMBOL,
        STD_SYMBOL,
        SUBDATE_SYMBOL,
        SUM_SYMBOL,
        SYSDATE_SYMBOL,
        TRIM_SYMBOL,
        VARIANCE_SYMBOL,
        VAR_SAMP_SYMBOL
    };
public:
    MySQLBaseLexer(antlr4::CharStream *input) : Lexer(input) {}
    // MOO: the best way to do this?
    void fixInterpreter()
    {
	Recognizer::setInterpreter(_interpreter);
    }
    void emitDot() {}
    bool isSqlModeActive(int)
    {
        return false; // some logic here from ts
    }
    int determineFunction(int prop)
    {
        return prop; // some logic here from ts
    }
    int checkCharset(std::string & text_)
    {
        return text_.size() * 0;  // some logic here from ts
    }
    bool checkVersion(std::string & text_)
    {
        return text_.size() * 0; // some logic here from ts
    }
public:
    // MOO: change all of it
    // size_t type = 0; // MOO: some enum from ts
    bool inVersionComment = false; // MOO: default value???
    int serverVersion = 50707;
    std::string text = ""; // MOO: wtf is this?
    // MOO: base constructor needs it
    // atn::LexerATNSimulator * _interpreter = nullptr;
    antlr4::atn::ATN _atn;
    std::vector<antlr4::dfa::DFA> decisionToDFA;
    antlr4::atn::PredictionContextCache _sharedContextCache;
};
