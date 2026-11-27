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

// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging

parser grammar PromQLParser;

options {
    tokenVocab = PromQLLexer;
}

expression
    : vectorOperation EOF
    ;

// Binary operations are ordered by precedence

// Unary operations have the same precedence as multiplications

vectorOperation
    : <assoc = right> vectorOperation subqueryOp
    | <assoc = right> vectorOperation powOp vectorOperation
    | unaryOp vectorOperation
    | vectorOperation multOp vectorOperation
    | vectorOperation addOp vectorOperation
    | vectorOperation compareOp vectorOperation
    | vectorOperation andUnlessOp vectorOperation
    | vectorOperation orOp vectorOperation
    | vector
    ;

// Operators

unaryOp
    : (ADD | SUB)
    ;

powOp
    : POW grouping?
    ;

multOp
    : (MULT | DIV | MOD | ATAN2) grouping?
    ;

addOp
    : (ADD | SUB) grouping?
    ;

compareOp
    : (DEQ | NE | GT | LT | GE | LE) BOOL? grouping?
    ;

andUnlessOp
    : (AND | UNLESS) grouping?
    ;

orOp
    : OR grouping?
    ;

subqueryOp
    : SUBQUERY_RANGE offsetOp?
    ;

offsetOp
    : AT timestamp (OFFSET offsetValue)?
    | OFFSET offsetValue (AT timestamp)?
    ;

vector
    : function_
    | aggregation
    | instantSelector
    | rangeSelector
    | selectorWithOffset
    | literal
    | parens
    ;

parens
    : LEFT_PAREN vectorOperation RIGHT_PAREN
    ;

// Timestamps and durations

timestamp
    : NUMBER
    | START LEFT_PAREN RIGHT_PAREN
    | END LEFT_PAREN RIGHT_PAREN
    ;

duration
    : NUMBER
    ;

offsetValue
    : (ADD | SUB)? NUMBER
    ;

// Selectors

instantSelector
    : metricName (LEFT_BRACE labelMatcherList? RIGHT_BRACE)?
    | LEFT_BRACE labelMatcherList RIGHT_BRACE
    ;

labelMatcher
    : labelName labelMatcherOperator STRING
    | STRING
    ;

labelMatcherOperator
    : EQ
    | NE
    | RE
    | NRE
    ;

labelMatcherList
    : labelMatcher (COMMA labelMatcher)* COMMA?
    ;

rangeSelector
    : instantSelector SELECTOR_RANGE
    ;

selectorWithOffset
    : instantSelector offsetOp
    | rangeSelector offsetOp
    ;

// Functions

function_
    : FUNCTION LEFT_PAREN (parameter (COMMA parameter)*)? RIGHT_PAREN
    | START LEFT_PAREN RIGHT_PAREN
    | END LEFT_PAREN RIGHT_PAREN
    ;

// A literal is a `vectorOperation` too, so a separate `literal` alternative would give a literal argument
// two parses, and the parser would resolve that with full-context prediction on every such argument.
parameter
    : vectorOperation
    ;

parameterList
    : LEFT_PAREN (parameter (COMMA parameter)*)? RIGHT_PAREN
    ;

// Aggregations

// `by` and `without` are factored out so that no two alternatives start with the same `parameterList`:
// to choose between such alternatives the parser would look ahead through the whole list.
aggregation
    : AGGREGATION_OPERATOR ((by | without) parameterList | parameterList (by | without)?)
    ;

by
    : BY LEFT_PAREN labelNameList RIGHT_PAREN
    ;

without
    : WITHOUT LEFT_PAREN labelNameList RIGHT_PAREN
    ;

// Vector one-to-one/one-to-many joins

grouping
    : (on_ | ignoring) (groupLeft | groupRight)?
    ;

on_
    : ON LEFT_PAREN labelNameList RIGHT_PAREN
    ;

ignoring
    : IGNORING LEFT_PAREN labelNameList RIGHT_PAREN
    ;

// `(` after `group_left` or `group_right` could also start the right operand. As in Prometheus, it always starts
// the label list: the lexer makes one token of the keyword and that `(`, so the alternatives start with different
// tokens and the parser doesn't look ahead to choose between them.
groupLeft
    : GROUP_LEFT_PAREN labelNameList RIGHT_PAREN
    | GROUP_LEFT
    ;

groupRight
    : GROUP_RIGHT_PAREN labelNameList RIGHT_PAREN
    | GROUP_RIGHT
    ;

// Label names

labelName
    : keyword
    | METRIC_NAME
    | LABEL_NAME
    | STRING
    ;

labelNameList
    : (labelName (COMMA labelName)* COMMA?)?
    ;

metricName
    : METRIC_NAME
    | metricNameKeyword
    ;

// Unlike a label name, a metric name can't be `bool`, `on`, `ignoring`, `group_left`, `group_right` or `atan2`,
// as in Prometheus. Otherwise `1 == bool -abs(up)` would have two parses, `bool` being a modifier or a metric,
// and to choose between them the parser would look ahead through the whole operand, however deeply it is nested.
metricNameKeyword
    : AND
    | OR
    | UNLESS
    | BY
    | WITHOUT
    | OFFSET
    | START
    | END
    | AGGREGATION_OPERATOR
    | FUNCTION
    ;

keyword
    : AND
    | OR
    | UNLESS
    | ATAN2
    | BY
    | WITHOUT
    | ON
    | IGNORING
    | GROUP_LEFT
    | GROUP_RIGHT
    | OFFSET
    | BOOL
    | START
    | END
    | AGGREGATION_OPERATOR
    | FUNCTION
    ;

literal
    : NUMBER
    | STRING
    ;
