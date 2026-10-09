/*
 [The "BSD licence"]
 Copyright (c) 2013 Terence Parr
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:
 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
 3. The name of the author may not be used to endorse or promote products
    derived from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// $antlr-format alignTrailingComments true, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine true, allowShortBlocksOnASingleLine true, minEmptyLines 0, alignSemicolons ownLine
// $antlr-format alignColons trailing, singleLineOverrulesHangingColon true, alignLexerCommands true, alignLabels true, alignTrailers true

lexer grammar PromQLLexer;

channels {
    WHITESPACE,
    COMMENTS
}

// All keywords in PromQL are case insensitive, it is just function,
// label and metric names that are not.
options {
    caseInsensitive = true;
}

fragment INTEGER_NUMBER: [0-9]+ ('_'? [0-9]+)*;

fragment FLOATING_POINT: (INTEGER_NUMBER '.' INTEGER_NUMBER?) | ('.' INTEGER_NUMBER);

fragment SCIENTIFIC_NUMBER: (INTEGER_NUMBER | FLOATING_POINT) 'e' [-+]? INTEGER_NUMBER;

fragment INF: 'inf';
fragment NAN: 'nan';

fragment HEXADECIMAL_NUMBER: '0x' ('_'? [0-9A-F]+)+;

// The proper order (longest to the shortest) must be validated after parsing
fragment DURATION_FORMAT options {
    caseInsensitive = false;
}:
    ([0-9]+ ('ms' | [smhdwy]))+
;

NUMBER: INTEGER_NUMBER | FLOATING_POINT | SCIENTIFIC_NUMBER | INF | NAN | HEXADECIMAL_NUMBER | DURATION_FORMAT;

STRING: '\'' (~('\'' | '\\') | '\\' .)* '\'' | '"' (~('"' | '\\') | '\\' .)* '"' | '`' (~'`')* '`';

// Binary operators

ADD  : '+';
SUB  : '-';
MULT : '*';
DIV  : '/';
MOD  : '%';
POW  : '^';

AND    : 'and';
OR     : 'or';
UNLESS : 'unless';

// Comparison operators

EQ  : '=';
DEQ : '==';
NE  : '!=';
GT  : '>';
LT  : '<';
GE  : '>=';
LE  : '<=';
RE  : '=~';
NRE : '!~';

// Aggregation modifiers

BY      : 'by';
WITHOUT : 'without';

// Join modifiers

ON          : 'on';
IGNORING    : 'ignoring';
GROUP_LEFT  : 'group_left';
GROUP_RIGHT : 'group_right';

OFFSET: 'offset';

BOOL: 'bool';

AGGREGATION_OPERATOR:
    'sum'
    | 'min'
    | 'max'
    | 'avg'
    | 'group'
    | 'stddev'
    | 'stdvar'
    | 'count'
    | 'count_values'
    | 'bottomk'
    | 'topk'
    | 'quantile'
;

FUNCTION options {
    caseInsensitive = false;
}:
    'abs'
    | 'absent'
    | 'absent_over_time'
    | 'ceil'
    | 'changes'
    | 'clamp'
    | 'clamp_max'
    | 'clamp_min'
    | 'day_of_month'
    | 'day_of_week'
    | 'day_of_year'
    | 'days_in_month'
    | 'delta'
    | 'deriv'
    | 'exp'
    | 'floor'
    | 'histogram_count'
    | 'histogram_sum'
    | 'histogram_fraction'
    | 'histogram_quantile'
    | 'holt_winters'
    | 'hour'
    | 'idelta'
    | 'increase'
    | 'irate'
    | 'label_join'
    | 'label_replace'
    | 'ln'
    | 'log2'
    | 'log10'
    | 'minute'
    | 'month'
    | 'predict_linear'
    | 'rate'
    | 'resets'
    | 'round'
    | 'scalar'
    | 'sgn'
    | 'sort'
    | 'sort_desc'
    | 'sqrt'
    | 'time'
    | 'timestamp'
    | 'vector'
    | 'year'
    | 'avg_over_time'
    | 'min_over_time'
    | 'max_over_time'
    | 'sum_over_time'
    | 'count_over_time'
    | 'quantile_over_time'
    | 'stddev_over_time'
    | 'stdvar_over_time'
    | 'last_over_time'
    | 'present_over_time'
    | 'acos'
    | 'acosh'
    | 'asin'
    | 'asinh'
    | 'atan'
    | 'atanh'
    | 'cos'
    | 'cosh'
    | 'sin'
    | 'sinh'
    | 'tan'
    | 'tanh'
    | 'deg'
    | 'pi'
    | 'rad'
;

LEFT_BRACE  : '{';
RIGHT_BRACE : '}';

LEFT_PAREN  : '(';
RIGHT_PAREN : ')';

LEFT_BRACKET  : '[';
RIGHT_BRACKET : ']';

COMMA: ',';

AT: '@';

SUBQUERY_RANGE: LEFT_BRACKET WS_FRAGMENT? NUMBER WS_FRAGMENT? ':' WS_FRAGMENT? NUMBER? WS_FRAGMENT? RIGHT_BRACKET;

SELECTOR_RANGE: LEFT_BRACKET WS_FRAGMENT? NUMBER WS_FRAGMENT? RIGHT_BRACKET;

METRIC_NAME : [a-z_:] [a-z0-9_:]*;
LABEL_NAME  : [a-z_] [a-z0-9_]*;

WS         : [\r\t\n ]+   -> channel(WHITESPACE);
SL_COMMENT : '#' .*? '\n' -> channel(COMMENTS);

// Whitespace as a fragment (so it can be used as a part of another token).
fragment WS_FRAGMENT: [\r\t\n ]+;
