// $antlr-format alignTrailingComments true, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine true, allowShortBlocksOnASingleLine true, minEmptyLines 0, alignSemicolons ownLine
// $antlr-format alignColons trailing, singleLineOverrulesHangingColon true, alignLexerCommands true, alignLabels true, alignTrailers true

lexer grammar SparqlLexer;

options {
    caseInsensitive = true;
}

// Keywords

A options {
    caseInsensitive = false;
}: 'a';

ASC         : 'ASC';
ASK         : 'ASK';
BASE        : 'BASE';
BOUND       : 'BOUND';
BY          : 'BY';
CONSTRUCT   : 'CONSTRUCT';
DATATYPE    : 'DATATYPE';
DESC        : 'DESC';
DESCRIBE    : 'DESCRIBE';
DISTINCT    : 'DISTINCT';
FILTER      : 'FILTER';
FROM        : 'FROM';
GRAPH       : 'GRAPH';
LANG        : 'LANG';
LANGMATCHES : 'LANGMATCHES';
LIMIT       : 'LIMIT';
NAMED       : 'NAMED';
OFFSET      : 'OFFSET';
OPTIONAL    : 'OPTIONAL';
ORDER       : 'ORDER';
PREFIX      : 'PREFIX';
REDUCED     : 'REDUCED';
REGEX       : 'REGEX';
SELECT      : 'SELECT';
STR         : 'STR';
UNION       : 'UNION';
WHERE       : 'WHERE';
TRUE        : 'true';
FALSE       : 'false';

IS_LITERAL : 'isLITERAL';
IS_BLANK   : 'isBLANK';
IS_URI     : 'isURI';
IS_IRI     : 'isIRI';
SAME_TERM  : 'sameTerm';

// OPERATORS

COMMA            : ',';
DOT              : '.';
DOUBLE_AMP       : '&&';
DOUBLE_BAR       : '||';
DOUBLE_CARET     : '^^';
EQUAL            : '=';
EXCLAMATION      : '!';
GREATER          : '>';
GREATER_OR_EQUAL : '>=';
LESS             : '<';
LESS_OR_EQUAL    : '<=';
L_CURLY          : '{';
L_PAREN          : '(';
L_SQUARE         : '[';
MINUS            : '-';
NOT_EQUAL        : '!=';
PLUS             : '+';
R_CURLY          : '}';
R_PAREN          : ')';
R_SQUARE         : ']';
SEMICOLON        : ';';
SLASH            : '/';
STAR             : '*';

// MISC

IRI_REF: '<' (~('<' | '>' | '"' | '{' | '}' | '|' | '^' | '\\' | '`') | PN_CHARS)* '>';

PNAME_NS: PN_PREFIX? ':';

PNAME_LN: PNAME_NS PN_LOCAL;

BLANK_NODE_LABEL: '_:' PN_LOCAL;

VAR1: '?' VARNAME;

VAR2: '$' VARNAME;

LANGTAG: '@' PN_CHARS_BASE+ ('-' (PN_CHARS_BASE | DIGIT)+)*;

INTEGER: DIGIT+;

DECIMAL: DIGIT+ '.' DIGIT* | '.' DIGIT+;

DOUBLE: DIGIT+ '.' DIGIT* EXPONENT | '.'? DIGIT+ EXPONENT;

INTEGER_POSITIVE: '+' INTEGER;

DECIMAL_POSITIVE: '+' DECIMAL;

DOUBLE_POSITIVE: '+' DOUBLE;

INTEGER_NEGATIVE: '-' INTEGER;

DECIMAL_NEGATIVE: '-' DECIMAL;

DOUBLE_NEGATIVE: '-' DOUBLE;

EXPONENT: 'E' ('+' | '-')? DIGIT+;

STRING_LITERAL1: '\'' (~('\u0027' | '\u005C' | '\u000A' | '\u000D') | ECHAR)* '\'';

STRING_LITERAL2: '"' (~('\u0022' | '\u005C' | '\u000A' | '\u000D') | ECHAR)* '"';

STRING_LITERAL_LONG1: '\'\'\'' (('\'' | '\'\'')? (~('\'' | '\\') | ECHAR))* '\'\'\'';

STRING_LITERAL_LONG2: '"""' (( '"' | '""')? (~('\'' | '\\') | ECHAR))* '"""';

ECHAR options {
    caseInsensitive = false;
}: '\\' ('t' | 'b' | 'n' | 'r' | 'f' | '"' | '\'');

NIL: '(' WS* ')';

ANON: '[' WS* ']';

PN_CHARS_U: PN_CHARS_BASE | '_';

VARNAME:
    (PN_CHARS_U | DIGIT) (
        PN_CHARS_U
        | DIGIT
        | '\u00B7'
        | '\u0300' ..'\u036F'
        | '\u203F' ..'\u2040'
    )*
;

fragment PN_CHARS:
    PN_CHARS_U
    | '-'
    | DIGIT
    /*| '\u00B7'
    | '\u0300'..'\u036F'
    | '\u203F'..'\u2040'*/
;

PN_PREFIX: PN_CHARS_BASE ((PN_CHARS | '.')* PN_CHARS)?;

PN_LOCAL: (PN_CHARS_U | DIGIT) ((PN_CHARS | '.')* PN_CHARS)?;

fragment PN_CHARS_BASE options {
    caseInsensitive = false;
}:
    'A' ..'Z'
    | 'a' ..'z'
    | '\u00C0' ..'\u00D6'
    | '\u00D8' ..'\u00F6'
    | '\u00F8' ..'\u02FF'
    | '\u0370' ..'\u037D'
    | '\u037F' ..'\u1FFF'
    | '\u200C' ..'\u200D'
    | '\u2070' ..'\u218F'
    | '\u2C00' ..'\u2FEF'
    | '\u3001' ..'\uD7FF'
    | '\uF900' ..'\uFDCF'
    | '\uFDF0' ..'\uFFFD'
;

fragment DIGIT: '0' ..'9';

WS: [ \t\n\r]+ -> channel(HIDDEN);