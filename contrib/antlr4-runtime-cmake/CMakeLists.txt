set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/antlr4-runtime)

set (SRCS
    ${LIBRARY_DIR}/ANTLRErrorListener.cpp
    ${LIBRARY_DIR}/ANTLRErrorStrategy.cpp
    ${LIBRARY_DIR}/ANTLRFileStream.cpp
    ${LIBRARY_DIR}/ANTLRInputStream.cpp
    ${LIBRARY_DIR}/BailErrorStrategy.cpp
    ${LIBRARY_DIR}/BaseErrorListener.cpp
    ${LIBRARY_DIR}/BufferedTokenStream.cpp
    ${LIBRARY_DIR}/CharStream.cpp
    ${LIBRARY_DIR}/CommonToken.cpp
    ${LIBRARY_DIR}/CommonTokenFactory.cpp
    ${LIBRARY_DIR}/CommonTokenStream.cpp
    ${LIBRARY_DIR}/ConsoleErrorListener.cpp
    ${LIBRARY_DIR}/DefaultErrorStrategy.cpp
    ${LIBRARY_DIR}/DiagnosticErrorListener.cpp
    ${LIBRARY_DIR}/Exceptions.cpp
    ${LIBRARY_DIR}/FailedPredicateException.cpp
    ${LIBRARY_DIR}/InputMismatchException.cpp
    ${LIBRARY_DIR}/InterpreterRuleContext.cpp
    ${LIBRARY_DIR}/IntStream.cpp
    ${LIBRARY_DIR}/Lexer.cpp
    ${LIBRARY_DIR}/LexerInterpreter.cpp
    ${LIBRARY_DIR}/LexerNoViableAltException.cpp
    ${LIBRARY_DIR}/ListTokenSource.cpp
    ${LIBRARY_DIR}/NoViableAltException.cpp
    ${LIBRARY_DIR}/Parser.cpp
    ${LIBRARY_DIR}/ParserInterpreter.cpp
    ${LIBRARY_DIR}/ParserRuleContext.cpp
    ${LIBRARY_DIR}/ProxyErrorListener.cpp
    ${LIBRARY_DIR}/RecognitionException.cpp
    ${LIBRARY_DIR}/Recognizer.cpp
    ${LIBRARY_DIR}/RuleContext.cpp
    ${LIBRARY_DIR}/RuleContextWithAltNum.cpp
    ${LIBRARY_DIR}/RuntimeMetaData.cpp
    ${LIBRARY_DIR}/Token.cpp
    ${LIBRARY_DIR}/TokenSource.cpp
    ${LIBRARY_DIR}/TokenStream.cpp
    ${LIBRARY_DIR}/TokenStreamRewriter.cpp
    ${LIBRARY_DIR}/UnbufferedCharStream.cpp
    ${LIBRARY_DIR}/UnbufferedTokenStream.cpp
    ${LIBRARY_DIR}/Vocabulary.cpp
    ${LIBRARY_DIR}/WritableToken.cpp
)

add_library (antlr4-runtime ${SRCS})

target_include_directories (antlr4-runtime SYSTEM PUBLIC ${LIBRARY_DIR})
