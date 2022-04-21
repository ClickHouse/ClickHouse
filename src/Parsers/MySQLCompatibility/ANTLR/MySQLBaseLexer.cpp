#include "MySQLLexer.h"

void MySQLBaseLexer::emitDot()
{
	std::unique_ptr<antlr4::CommonToken> token = this->_factory->create(
		{this, this->_input},
		MySQLLexer::DOT_SYMBOL,
		".",
		this->channel,
		this->tokenStartCharIndex,
		this->tokenStartCharIndex,
		this->tokenStartLine,
		this->tokenStartCharPositionInLine
	);
	
	pendingTokens.push_back(std::move(token));

	this->tokenStartCharIndex++;
}

int MySQLBaseLexer::checkCharset(const std::string & text)
{
	return (std::find(charsets.begin(), charsets.end(), text) != charsets.end()) ?
		MySQLLexer::UNDERSCORE_CHARSET : MySQLLexer::IDENTIFIER; 
}

int MySQLBaseLexer::determineFunction(int proposed)
{
	if (isSqlModeActive(SqlMode::IgnoreSpace))
	{
		auto input = this->_input->LA(1);
		auto character = static_cast<char>(input); // MOO
		while (character == ' ' || character == '\t' || character == '\r' || character == '\n')
		{
			// LexerATNSimulator
			this->getInterpreter<antlr4::atn::LexerATNSimulator>()->consume(this->_input);
			this->channel = antlr4::Lexer::HIDDEN;
			this->type = MySQLLexer::WHITESPACE;

			input = this->_input->LA(1);
			character = static_cast<char>(input); // MOO
		}
		input = this->_input->LA(1);

		return static_cast<char>(input) == '(' ? proposed : MySQLLexer::IDENTIFIER;
	}
}
