#pragma once

#include "antlr4-runtime.h"
#include "SqlMode.h"

// using namespace antlr4;
// using namespace antlr4::atn;

class MySQLBaseLexer : public antlr4::Lexer
{
public:
    MySQLBaseLexer(antlr4::CharStream *input) : Lexer(input) {}
	virtual void reset()
	{
		inVersionComment = false;
		antlr4::Lexer::reset();
	}
	virtual std::unique_ptr<antlr4::Token> nextToken()
	{
		if (!pendingTokens.empty())
			return shiftToken();
		
		auto next = std::move(antlr4::Lexer::nextToken());

		if (!pendingTokens.empty())
		{
			pendingTokens.push_back(std::move(next));
			return shiftToken();
		}

		return next;
	}

	// nextDefaultChannelToken
    bool checkVersion(const std::string & text)
    {
    	if (text.size() < 8)
			return false;

		auto version = std::stoi(text.substr(3));
		if (version <= serverVersion)
		{
			inVersionComment = true;
			return true;
		}
		
		return false;
	}
	
	int checkCharset(const std::string & text);

	void emitDot();
    bool isSqlModeActive(int mode)
    {
		if (!sqlMode)
			return false;
		
		return (sqlMode & mode) != 0;
    }
    int determineFunction(int prop); 
    
private:
	std::unique_ptr<antlr4::Token> shiftToken()
	{
		auto token = std::move(pendingTokens.front());
		pendingTokens.pop_front();
		return token;
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
private:
	std::deque<std::unique_ptr<antlr4::Token> > pendingTokens;
	std::vector<std::string> charsets;
	SqlMode sqlMode;
};
